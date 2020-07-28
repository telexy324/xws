package xws

import (
	"errors"
	"fmt"
	uuid "github.com/iris-contrib/go.uuid"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultReadTimeout = 3 * time.Second
const defaultWriteTimeout = 3 * time.Second

type Upgrader func(w http.ResponseWriter, r *http.Request) (Socket, error)

type IDGenerator func(w http.ResponseWriter, r *http.Request) string

var DefaultIDGenerator IDGenerator = func(http.ResponseWriter, *http.Request) string {
	id, err := uuid.NewV4()
	if err != nil {
		return strconv.FormatInt(time.Now().Unix(), 10)
	}
	return id.String()
}

type Server struct {
	uuid string

	upgrader    Upgrader
	IDGenerator IDGenerator

	mu sync.RWMutex

	// connection read/write timeouts.
	readTimeout  time.Duration
	writeTimeout time.Duration

	count uint64

	connections       map[*Conn]struct{}
	connect           chan *Conn
	disconnect        chan *Conn
	actions           chan action
	broadcastMessages chan []Message

	broadcaster *broadcaster

	closed uint32

	OnUpgradeError func(err error)
	// OnConnect can be optionally registered to be notified for any new neffos client connection,
	// it can be used to force-connect a client to a specific namespace(s) or to send data immediately or
	// even to cancel a client connection and dissalow its connection when its return error value is not nil.
	// Don't confuse it with the `OnNamespaceConnect`, this callback is for the entire client side connection.
	OnConnect func(c *Conn) error
	// OnDisconnect can be optionally registered to notify about a connection's disconnect.
	// Don't confuse it with the `OnNamespaceDisconnect`, this callback is for the entire client side connection.
	OnDisconnect func(c *Conn)
}

func New(upgrader Upgrader, opts ...ServerOption) *Server {
	s := &Server{
		uuid:              uuid.Must(uuid.NewV4()).String(),
		upgrader:          upgrader,
		readTimeout:       defaultReadTimeout,
		writeTimeout:      defaultWriteTimeout,
		connections:       make(map[*Conn]struct{}),
		connect:           make(chan *Conn, 1),
		disconnect:        make(chan *Conn),
		actions:           make(chan action),
		broadcastMessages: make(chan []Message),
		broadcaster:       newBroadcaster(),
		waitingMessages:   make(map[string]chan Message),
		IDGenerator:       DefaultIDGenerator,
	}

	for _, opt := range opts {
		opt(s)
	}

	go s.start()

	return s
}

type ServerOption func(*Server)

func (s *Server) start() {
	atomic.StoreUint32(&s.closed, 0)

	for {
		select {
		case c := <-s.connect:
			s.connections[c] = struct{}{}
			atomic.AddUint64(&s.count, 1)
		case c := <-s.disconnect:
			if _, ok := s.connections[c]; ok {
				// close(c.out)
				delete(s.connections, c)
				atomic.AddUint64(&s.count, ^uint64(0))
				// println("disconnect...")
				if s.OnDisconnect != nil {
					s.OnDisconnect(c)
				}
			}
		case msgs := <-s.broadcastMessages:
			for c := range s.connections {
				publishMessages(c, msgs)
			}
		case act := <-s.actions:
			for c := range s.connections {
				act.call(c)
			}

			if act.done != nil {
				act.done <- struct{}{}
			}
		}
	}
}

func (s *Server) Close() {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		s.Do(func(c *Conn) {
			c.Close()
		}, false)
	}
}

var (
	errServerClosed  = errors.New("server closed")
	errInvalidMethod = errors.New("no valid request method")
)

func (s *Server) Upgrade(
	w http.ResponseWriter,
	r *http.Request,
	socketWrapper func(Socket) Socket,
	customIDGen IDGenerator,
) (*Conn, error) {
	if atomic.LoadUint32(&s.closed) > 0 {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return nil, errServerClosed
	}

	if r.Method != http.MethodGet {
		// RCF rfc2616 https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
		// The response MUST include an Allow header containing a list of valid methods for the requested resource.
		//
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Allow#Examples
		w.Header().Set("Allow", http.MethodGet)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintln(w, http.StatusText(http.StatusMethodNotAllowed))
		return nil, errInvalidMethod
	}

	socket, err := s.upgrader(w, r)
	if err != nil {
		if s.OnUpgradeError != nil {
			s.OnUpgradeError(err)
		}
		return nil, err
	}

	if socketWrapper != nil {
		socket = socketWrapper(socket)
	}

	c := newConn(socket, s.namespaces)
	if customIDGen != nil {
		c.id = customIDGen(w, r)
	} else {
		c.id = s.IDGenerator(w, r)
	}
	c.serverConnID = genServerConnID(s, c)

	c.readTimeout = s.readTimeout
	c.writeTimeout = s.writeTimeout
	c.server = s

	retriesHeaderValue := r.Header.Get(websocketReconectHeaderKey)
	if retriesHeaderValue != "" {
		c.ReconnectTries, _ = strconv.Atoi(retriesHeaderValue)
	}

	if !s.usesStackExchange() && !s.SyncBroadcaster {
		go func(c *Conn) {
			for s.waitMessages(c) {
			}
		}(c)
	}

	s.connect <- c

	go c.startReader()

	// Before `OnConnect` in order to be able
	// to Broadcast inside the `OnConnect` custom func.
	if s.usesStackExchange() {
		if err := s.StackExchange.OnConnect(c); err != nil {
			c.readiness.unwait(err)
			return nil, err
		}
	}

	// Start the reader before `OnConnect`, remember clients may remotely connect to namespace before `Server#OnConnect`
	// therefore any `Server:NSConn#OnNamespaceConnected` can write immediately to the client too.
	// Note also that the `Server#OnConnect` itself can do that as well but if the written Message's Namespace is not locally connected
	// it, correctly, can't pass the write checks. Also, and most important, the `OnConnect` is ready to connect a client to a namespace (locally and remotely).
	//
	// This has a downside:
	// We need a way to check if the `OnConnect` returns an non-nil error which means that the connection should terminate before namespace connect or anything.
	// The solution is to still accept reading messages but add them to the queue(like we already do for any case messages came before ack),
	// the problem to that is that the queue handler is fired when ack is done but `OnConnect` may not even return yet, so we introduce a `mark ready` atomic scope
	// and a channel which will wait for that `mark ready` if handle queue is called before ready.
	// Also make the same check before emit the connection's disconnect event (if defined),
	// which will be always ready to be called because we added the connections via the connect channel;
	// we still need the connection to be available for any broadcasting on connected events.
	// ^ All these only when server-side connection in order to correctly handle the end-developer's `OnConnect`.
	//
	// Look `Conn.serverReadyWaiter#startReader##handleQueue.serverReadyWaiter.unwait`(to hold the events until no error returned or)
	// `#Write:serverReadyWaiter.unwait` (for things like server connect).
	// All cases tested & worked perfectly.
	if s.OnConnect != nil {
		if err = s.OnConnect(c); err != nil {
			// TODO: Do something with that error.
			// The most suitable thing we can do is to somehow send this to the client's `Dial` return statement.
			// This can be done if client waits for "OK" signal or a failure with an error before return the websocket connection,
			// as for today we have the ack process which does NOT block and end-developer can send messages and server will handle them when both sides are ready.
			// So, maybe it's a better solution to transform that process into a blocking state which can handle any `Server#OnConnect` error and return it at client's `Dial`.
			// Think more later today.
			// Done but with a lot of code.... will try to cleanup some things.
			//println("OnConnect error: " + err.Error())
			c.readiness.unwait(err)
			// No need to disconnect here, connection's .Close will be called on readiness ch errored.

			// c.Close()
			return nil, err
		}
	}

	//println("OnConnect does not exist or no error, fire unwait")
	c.readiness.unwait(nil)

	return c, nil
}