package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config stores tcp server properties
//配置文件，规定了地址、最大连接数和超时时间
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
//实现带有优雅关闭的网络监听器，地址配置于cfg文件中
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
//接受监听器，并使用给定的handler处理这个连接，在接收到停止信号时会进行优雅关闭。
//值得注意的是，这个函数中创建了一个context，并且对于每一个handler处理的连接，都会把这个context传进去。
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	//开一个监听停止信号的协程，一旦closeChan接收到到停止信号，立马关闭监听器。
	//关闭后，监听器的Accept（）方法将会返回err
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		//防止新的连接进来
		_ = listener.Close() // listener.Accept() will return err immediately
		//关闭旧的连接
		_ = handler.Close() // close connections
	}()

	// listen port
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		// handle
		logger.Info("accept link")
		//确保执行中的连接不被误关闭
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
