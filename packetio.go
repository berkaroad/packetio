package packetio

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"errors"
	"sync"
)

var consoleLog = log.New(os.Stdout, "[packetio] ", log.LstdFlags)

// DEBUG is switcher for debug
var DEBUG = false

const (
	defaultReaderSize int = 8 * 1024
	maxPayloadLen     int = 1<<24 - 1
)

// Error Code
var (
	ErrBadConn = errors.New("bad conn")
)

// PacketIO is a packet transfer on network.
type PacketIO struct {
	rb       *bufio.Reader
	wb       io.Writer
	sequence uint8
	rl       *sync.Mutex
	wl       *sync.Mutex
}

// New is to create PacketIO
func New(conn net.Conn) *PacketIO {
	p := new(PacketIO)
	p.rb = bufio.NewReaderSize(conn, defaultReaderSize)
	p.wb = conn
	p.sequence = 0
	p.rl = new(sync.Mutex)
	p.wl = new(sync.Mutex)
	return p
}

// ReadPacket is to read packet.
func (p *PacketIO) ReadPacket() ([]byte, error) {
	defer p.rl.Unlock()
	p.rl.Lock()

	p.sequence = 0
	header := []byte{0, 0, 0, 0}
	if _, err := io.ReadFull(p.rb, header); err != nil {
		header = nil
		if DEBUG {
			consoleLog.Println(err)
		}
		return nil, ErrBadConn
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		header = nil
		return nil, fmt.Errorf("invalid payload length %d", length)
	}
	sequence := uint8(header[3])
	if sequence != p.sequence {
		header = nil
		return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.sequence)
	}
	p.sequence++

	total := make([]byte, length)
	if _, err := io.ReadFull(p.rb, total); err != nil {
		header = nil
		total = nil
		if DEBUG {
			consoleLog.Println(err)
		}
		return nil, ErrBadConn
	}

	for length == maxPayloadLen {
		header = []byte{0, 0, 0, 0}
		if _, err := io.ReadFull(p.rb, header); err != nil {
			header = nil
			total = nil
			if DEBUG {
				consoleLog.Println(err)
			}
			return nil, ErrBadConn
		}
		length = int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		if length < 1 {
			header = nil
			total = nil
			return nil, fmt.Errorf("invalid payload length %d", length)
		}
		sequence = uint8(header[3])
		if sequence != p.sequence {
			header = nil
			total = nil
			return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.sequence)
		}
		p.sequence++

		data := make([]byte, length)
		if _, err := io.ReadFull(p.rb, data); err != nil {
			header = nil
			total = nil
			data = nil
			if DEBUG {
				consoleLog.Println(err)
			}
			return nil, ErrBadConn
		}
		total = append(total, data...)
		data = nil
	}
	if DEBUG {
		consoleLog.Println("ReadPacket", total)
	}
	return total, nil
}

// WritePacket is to write packet.
func (p *PacketIO) WritePacket(data []byte) error {
	defer p.wl.Unlock()
	p.wl.Lock()

	p.sequence = 0
	length := len(data)
	for length >= maxPayloadLen {
		buffer := make([]byte, 4, 4+maxPayloadLen)
		buffer[0] = 0xff
		buffer[1] = 0xff
		buffer[2] = 0xff
		buffer[3] = p.sequence
		buffer = append(buffer, data[:maxPayloadLen]...)

		if n, err := p.wb.Write(buffer); err != nil {
			buffer = nil
			if DEBUG {
				consoleLog.Println(err)
			}
			return ErrBadConn
		} else if n != (4 + maxPayloadLen) {
			buffer = nil
			return ErrBadConn
		} else {
			p.sequence++
			length -= maxPayloadLen
			data = data[maxPayloadLen:]
		}
	}
	buffer := make([]byte, 4, 4+length)
	buffer[0] = byte(length)
	buffer[1] = byte(length >> 8)
	buffer[2] = byte(length >> 16)
	buffer[3] = p.sequence
	buffer = append(buffer, data...)
	if n, err := p.wb.Write(buffer); err != nil {
		buffer = nil
		if DEBUG {
			consoleLog.Println(err)
		}
		return ErrBadConn
	} else if n != len(data)+4 {
		buffer = nil
		return ErrBadConn
	} else {
		p.sequence++
		if DEBUG {
			consoleLog.Println("WritePacket", append(buffer, data...))
		}
		return nil
	}
}

// WritePacketBatch is to write packet in batch
func (p *PacketIO) WritePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	defer p.wl.Unlock()
	p.wl.Lock()

	p.sequence = 0
	if data == nil {
		if direct == true {
			n, err := p.wb.Write(total)
			if err != nil {
				total = nil
				if DEBUG {
					consoleLog.Println(err)
				}
				return nil, ErrBadConn
			}
			if n != len(total) {
				total = nil
				return nil, ErrBadConn
			}
			if DEBUG {
				consoleLog.Println("WritePacketBatch", total)
			}
		}
		return total, nil
	}

	length := len(data)
	for length >= maxPayloadLen {
		header := []byte{0xff, 0xff, 0xff, p.sequence}
		total = append(total, header...)
		total = append(total, data[:maxPayloadLen]...)

		p.sequence++
		length -= maxPayloadLen
		data = data[maxPayloadLen:]
	}

	header := []byte{byte(length), byte(length >> 8), byte(length >> 16), p.sequence}
	total = append(total, header...)
	total = append(total, data[:maxPayloadLen]...)
	p.sequence++

	if direct {
		if n, err := p.wb.Write(total); err != nil {
			total = nil
			data = nil
			if DEBUG {
				consoleLog.Println(err)
			}
			return nil, ErrBadConn
		} else if n != len(total) {
			total = nil
			data = nil
			return nil, ErrBadConn
		}
		if DEBUG {
			consoleLog.Println("WritePacketBatch", total)
		}
	}
	return total, nil
}
