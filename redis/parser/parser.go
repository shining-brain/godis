package parser

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

//从给定的rawReader里读出编码并解析，结果塞入ch管道当中。
//关闭一个parse0好像只能通过读取到关闭信号？否则循环会一直执行！
func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		//recover用来捕获panic，防止进程真的完全结束了
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	//使用带有缓冲区的Reader来加速IO
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}

		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// there are some empty lines within replication traffic, ignore this error
			//protocolError(ch, "empty line")
			continue
		}
		//去掉尾部的"\r\n"
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				//从此处看出，simple string只用来传递状态信息
				Data: protocol.MakeStatusReply(content),
			}
			//这下面的和持久化策略相关，在此处暂时不管
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch)
				if err != nil {
					ch <- &Payload{Err: err}
					close(ch)
					return
				}
			}
		case '-':
			ch <- &Payload{
				//只返回和错误有的信息，是直接把这个字符串当作错误的信息本体
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

//处理BulkString类型的编码， Bulk String有两行，如：$3\r\nSET\r\n
//第一行类似消息头，第二行类似消息体。如果编码里表示字符串的长度是-1，则说明没有成功查到，返回了空value。
//解码完成后，直接丢进Payload管道
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	//第一个\r\n之前的字符串是接下来字符串的总长度
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		//-1是有特殊含义的，代表没有找到这个key对应的串
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	//消息体切片，+2是为了附带上\r\n
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		//最后记得清除掉\r\n
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

//和持久化有关，目前暂时不管
// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

//处理MultiBulk String类型的编码。
//以*开头，然后跟一个数字，换行。数字代表接下来有多少个BulkString，接下来的编码格式和BulkString一样。
//解码完成后，直接丢进Payload管道。
func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	//取出数组的长度
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		//以下的处理方法和对Bulk String的处理方法一摸一样
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			//为-1，则加入一个空切片
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

//发生了协议错误，比如MultiBulk String里必须要全是Bulk String，结果却存了个别的类型的和数据。
func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
