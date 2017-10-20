package main

import (
	"net"
	"os"
	"runtime/pprof"

	"time"

	"fmt"

	"sync"
	"sync/atomic"

	"github.com/mixer/redplex"
	"github.com/sirupsen/logrus"
)

var (
	benchData      = []byte("*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$345\r\n" + `{"channel":314,"id":"fd269030-aacd-11e7-b70f-fd0fddd98285","user_name":"Jeff","user_id":1355,"user_roles":["Pro","User"],"user_level":94,"user_avatar":"https://uploads.beam.pro/avatar/qryjcpn1-1355.jpg","message":{"message":[{"type":"text","data":"Finally. We're back on the computer.","text":"Finally. We're back on the computer."}],"meta":{}}}` + "\r\n")
	subscribe      = redplex.NewRequest("subscribe", 1).Append([]byte(`channel`)).Bytes()
	remoteAddress  = "127.0.0.1:3100"
	redplexAddress = "127.0.0.1:3101"
	benchedBytes   = 1024 * 1024 * 10
)

func main() {
	os.Remove(remoteAddress)
	os.Remove(redplexAddress)
	logrus.SetLevel(logrus.DebugLevel)

	benchListener, err := net.Listen("tcp", remoteAddress)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/bench: could not listen on the requested address")
		os.Exit(1)
	}
	redplexListener, err := net.Listen("tcp", redplexAddress)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/bench: could not listen on the requested address")
		os.Exit(1)
	}

	start := make(chan struct{})
	go serveBenchRemote(benchListener, start)
	go redplex.NewServer(redplexListener, redplex.NewPubsub(dialer, time.Second)).Listen()

	f, _ := os.Create("cpu.profile")
	pprof.StartCPUProfile(f)
	runBenchmark(start)
	pprof.StopCPUProfile()
}

func runBenchmark(start chan<- struct{}) {

	var (
		wg         sync.WaitGroup
		totalBytes int64
	)

	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			cnx, err := net.Dial("tcp", redplexAddress)
			if err != nil {
				logrus.WithError(err).Fatal("redplex/bench: error dialing to server")
			}

			if _, err := cnx.Write(subscribe); err != nil {
				logrus.WithError(err).Fatal("redplex/bench: error subscribing")
			}

			wg.Done()
			wg.Wait()

			buf := make([]byte, 32*1024)
			for {
				n, err := cnx.Read(buf)
				atomic.AddInt64(&totalBytes, int64(n))
				if err != nil {
					logrus.WithError(err).Fatal("redplex/bench: error in reader")
				}
			}
		}()
	}

	wg.Wait()
	start <- struct{}{}
	time.Sleep(time.Second) // give it a second to ramp up
	atomic.StoreInt64(&totalBytes, 0)

	go func() {
		last := int64(0)
		for {
			time.Sleep(time.Second)
			next := atomic.LoadInt64(&totalBytes)
			fmt.Println("delta", next-last)
			last = next
		}
	}()

	started := time.Now()
	time.Sleep(15 * time.Second)
	delta := time.Now().Sub(started)
	seconds := float64(delta) / float64(time.Second)
	gigabits := float64(totalBytes) / 1024 / 1024 * 8
	fmt.Printf("Read %d bytes in %.2fs (%.0f Mbps)\n", totalBytes, seconds, gigabits/seconds)
}

func serveBenchRemote(l net.Listener, start <-chan struct{}) {
	cnx, err := l.Accept()
	if err != nil {
		logrus.WithError(err).Fatal("redplex/bench: error accepting connection")
		os.Exit(1)
	}

	<-start

	toWrite := []byte{}
	for i := 0; i < 1000; i++ {
		toWrite = append(toWrite, benchData...)
	}

	for {
		cnx.Write(toWrite)
	}
}

func dialer() (net.Conn, error) { return net.Dial("tcp", remoteAddress) }
