/*
This package will receive a buffer that normally contains a JSON object and rebroadcast this
buffer at different interval to a UDP:Port destination.  For OWLSO the the UDP address should
be a network address so that information is broadcasted.  This will iliminate the need
for setting a static entry in the ARP table of the OWLSO-RELAY.
*/

package broadcaster

import (
	"errors"
	"github.com/antigloss/go/logger"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

/* true if the broadcaster is running
 */
var running = false

/* const use to indicate a packet need to be send now, at load time the broadcaster might
   be requested to send data at random interval to avoid overloading the network
   connection and sending the complete content of the database at once.  This const is
   use by other package that are calling the Update function.
*/
const BroadcastNow = 0

// On each LOW And HIGH Network component the MTU Should be set to 8192 or same value as
// the Const.  Because there are no devices between the two network card you should be
// able to set any MTU.  Most packet send by RELAY should be around 150b
const max_udp_packet_size = 8192

/* Channel for creating a list of item that need to be broadcasted, list contains pointer of item
 */
var broadcastChannel chan []byte

type item struct {
	/* bellow is data received by broadcaster, the broadcaster might send more information
	   but this is what we need everything else is extra and does not need to be sent to
	   couchdb, remember the application-server should only update the status of probes
	   and queries.  Those need to be manually define in the CouchDB by the client application.
	*/
	ID            string
	Buffer        []byte
	nextBroadcast int64 // private - do not broadcast
	broadcastStep int64 // private - do not broadcast
	probeIsDown   bool  // status of device, down device get broadcasted more often.
}

// List of Item currently being broadcasted.
var items map[string]*item

// lock to control the list of items
var mutexItems = &sync.RWMutex{}

// pool to reuse the items that goes in the list.
var pool = sync.Pool{
	// New creates an object when the pool has nothing available to return.
	// New must return an interface{} to make it flexible. You have to cast
	// your type after getting it.
	New: func() interface{} {
		// Pools often contain things like *bytes.Buffer, which are
		// temporary and re-usable.
		return &item{}
	},
}

/* get one item from the pool, item will be allocated but field Buffer len will be zero
 */
func getItemFromPool() *item {
	return pool.Get().(*item)
}

/* put back one item in the pool, clear the buffer that will no longer be use
 */
func putItemInPool(i *item) {
	// keep the .Buffer property so it can be reused later.
	pool.Put(i)
}

/* This is the main broadcast function that will at specific intervals broadcast all the devices
   Start the Broadcast function and send all the devices we know to the OWLSO-GUARD at specific
   intervals.  The information is send as a json message by threads that are created by this function.

   Broadcast_Multiplier = default 10
   ChannelSize = default 4096, for both the Update and Broadcast channel
   Default output IP 127.0.0.1
   Default output Port 512

*/
func Start(BroadcasterName string, ToIP string, ToPort, ChannelSize int, workercount int,
	Broadcast_Multiplier, broadcastmaxinterval, broadcastmaxintervalifdown, broadcastmininterval int64) error {

	// set to default if no IP provided,
	if ToIP == "" {
		ToIP = "127.0.0.1"
	}

	// set to default if port provided is invalid
	if ToPort <= 0 || ToPort > 65535 {
		ToPort = 512
	}

	// set default channel size if size if negative.
	if ChannelSize <= 0 {
		ChannelSize = 4096
	}

	// make sure there are at least 2 workers working.
	if workercount < 2 {
		workercount = 2
	}

	// set minimum value
	if Broadcast_Multiplier <= 0 {
		Broadcast_Multiplier = 2
	}

	// set minimum value
	if broadcastmaxinterval < 1 {
		broadcastmaxinterval = 1
	}

	// set minimum value
	if broadcastmaxintervalifdown < 1 {
		broadcastmaxintervalifdown = 1
	}

	// set minimum value
	if broadcastmininterval < 1 {
		broadcastmininterval = 1
	}

	logger.Info("Max UDP size set to " + strconv.Itoa(max_udp_packet_size) + " the MTU on Linux or Windows should be set to the same value or higher.")

	logger.Info("Starting Broadcaster " + BroadcasterName + " " + ToIP + ":" + strconv.Itoa(ToPort) + " channel size:" + strconv.Itoa(ChannelSize) + " threads:" + strconv.Itoa(workercount) +
		" broadcastMult: " + strconv.FormatInt(Broadcast_Multiplier, 10) + " broadcastMax: " + strconv.FormatInt(broadcastmaxinterval, 10) + " broadcastMaxIfDown: " + strconv.FormatInt(broadcastmaxintervalifdown, 10) +
		" broadcastMin: " + strconv.FormatInt(broadcastmininterval, 10))

	/* Allocate memory for holding the buffer items
	 */
	items = make(map[string]*item)

	// channel for sending the ID of packet that need to be broadcasted
	broadcastChannel = make(chan []byte, ChannelSize)

	/* create the udp worker threads that will send the udp packets
	 */
	for i := 0; i < workercount; i++ {
		go broadcastUDPThread(ToIP, ToPort)
	}

	/* monitor all items and flag the one that need to be broadcasted by removing them
	   from the map and inserting them into a Channel for broadcasting.
	*/
	go findItemThatNeedBroadcasting(Broadcast_Multiplier, broadcastmaxinterval, broadcastmaxintervalifdown, broadcastmininterval)

	running = true

	return nil
}

/* This function return a value if it's between limits value of the
   minimum or maximum allows value otherwise return the set limits.
*/
func minMax64(min, max, value int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

/* Insert or Replace an object in the database, each object is broadcasted
   at regular interval.  Buffer will be copy so that the calling function
   can do what they want with the buffer.  This is not the most efficient
   process but is safe.  randomStartMaxSecs would be use if the application
   load a database and is trowing 1000's at the same time.

*/
func Update(Id string, Buffer []byte, randomStartMaxSecs int64, isDown bool) {

	if !running {
		logger.Trace("Receive Update for " + Id + " but broadcaster as not been initialized yet.")
		return
	}

	//logger.Trace("Rx req broadcast ID = (" + Id + ") " + string(Buffer))

	/* either start broadcast now (0) or use a random number, when too many updates are
	   sent at the same time it's possible to set a random delay in seconds.
	   If the value is set to zero the data is broadcasted right away.
	*/
	next := time.Now().Unix()
	if randomStartMaxSecs > 0 {
		next += rand.Int63n(randomStartMaxSecs)
	}

	// there is no allocation here if getitemFromPool use an object from the pool
	newItem := getItemFromPool()
	newItem.ID = Id
	newItem.nextBroadcast = next
	newItem.broadcastStep = 0
	newItem.probeIsDown = isDown

	// copy the buffer into the object we got from the pool if the
	// buffer size is not large enough allocate more memory.
	needBYtes := len(Buffer)
	if cap(newItem.Buffer) < needBYtes {
		newItem.Buffer = make([]byte, needBYtes)
	}
	copy(newItem.Buffer, Buffer)

	/* if item is already in the map, put back the item in the pool and
	   overwrite the map to replace the ID so that it point to the new object.
	*/
	mutexItems.Lock()
	if old, found := items[newItem.ID]; found {
		putItemInPool(old)
		//logger.Trace("ID " + newItem.ID + " already existed replacing item")
	}
	items[newItem.ID] = newItem
	mutexItems.Unlock()

	//logger.Trace("done Receive Update() request for " + Id)
}

/* Remove a buffer from the broadcasting items and store it back in the pool.
 */
func Delete(Id string) error {

	logger.Trace("Receive Delete() request for " + Id)

	// lock the items map.
	mutexItems.Lock()
	defer mutexItems.Unlock()

	if item, ok := items[Id]; ok {
		// put item back in the pool.
		putItemInPool(item)
		// remove item from the map.
		delete(items, Id)
		return nil
	}

	return errors.New("delete " + Id + " ERROR NotFound")
}

/* each second go thru the items and find any item need to be broadcasted
 */
func findItemThatNeedBroadcasting(Broadcast_Multiplier, broadcastmaxinterval, broadcastmaxintervalifdown, broadcastmininterval int64) {

	for {

		currentTimeInSecs := time.Now().Unix()

		/* lock the map iterate items and look for item that need to be broadcasted
		   if item are found ID's must be added to the broadcastChannel
		*/

		mutexItems.Lock()

		/*	go thru all the items remove the one that need broadcasting and store
			them in the broadcast channel.  They will be put back in the map once
			the broadcast is complete.
			To summarize, the delete within a range is safe because the data is technically still there,
			but when it checks the tophash it sees that it can just skip over it and not include it in whatever
			range operation you're performing.

		*/
		for _, item := range items {

			if item.nextBroadcast < currentTimeInSecs {

				// found an item that need to be broadcasted copy the item buffer into a
				// temporary buffer.

				temp := make([]byte, len(item.Buffer))
				copy(temp, item.Buffer)

				// send the temporary buffer to the channel.

				select {
				case broadcastChannel <- temp:

					// item was successfully sent to the channel now set the next broadcast time,
					// down device get broadcasted more often.

					if !item.probeIsDown {
						item.broadcastStep = minMax64(broadcastmininterval, broadcastmaxinterval, item.broadcastStep*Broadcast_Multiplier)
					} else {
						item.broadcastStep = minMax64(broadcastmininterval, broadcastmaxintervalifdown, item.broadcastStep*Broadcast_Multiplier)
					}

					item.nextBroadcast = time.Now().Unix() + item.broadcastStep
					//logger.Trace("item was put in broadcast queue: " + item.ID + " { " + string(item.Buffer) + " } and it's nextBroadcast will be in " + strconv.FormatInt(item.broadcastStep, 10) + " secs")

				default:
				}
			}
		}
		mutexItems.Unlock()

		/* scan probes every 1 second.
		 */
		time.Sleep(time.Second)
	}
}

func getUDPSocketConn(minPort, maxPort int) (*net.UDPConn, int, error) {

	if minPort == 0 {
		return nil, 0, errors.New("Unable to use port 0")
	}
	if maxPort >= 65535 {
		maxPort = 65535
	}
	for i := minPort; i < maxPort; i++ {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: i})
		if err != nil {
			continue
		}
		return conn, i, nil
	}
	return nil, 0, errors.New("Unable to reserved a port.")
}

/* This thread go thru the list of items that need to be send, read the item and convert it  into
   a JSON format string this string is then sent to the OWLSO-GUARD using UDP to an IP or thru
   broadcast packet on a /30 network.
*/

func broadcastUDPThread(broadcastToIP string, broadcastToPort int) {

	if broadcastToIP == "" || (broadcastToPort >= 65535) || (broadcastToPort < 0) {
		logger.Error("broadcastUDPThread() broadcast IP or Port issue " + broadcastToIP + " " + strconv.Itoa(broadcastToPort))
		return
	}

	// get a free UDP port to create a socket
	conn, portNumber, err := getUDPSocketConn(10000, 65535)

	if err != nil {
		logger.Error(err.Error())
		return
	}

	if conn == nil {
		logger.Error("No port assigned for broadcastUDPThread")
		return
	}

	defer conn.Close()
	logger.Info("broadcastUDPThread created with UDP port " + strconv.Itoa(portNumber))

	destIP := net.ParseIP(broadcastToIP)

	/* loop until program end or channel close get the next ID to broadcast
	 */
	for item := range broadcastChannel {

		// no lock is require because the buffer item ptr has been removed from the map.

		/* make sure we actually have something to send; this item length is not empty
		 */
		msgsize := len(item)

		if msgsize > 0 && msgsize <= max_udp_packet_size {

			_, err := conn.WriteTo(item[:msgsize], &net.UDPAddr{IP: destIP, Port: broadcastToPort})
			if err != nil {
				logger.Error("Write:", err)
				continue
			}

		} /* end if msg len is > 0 */

	} // infinite for loop
}
