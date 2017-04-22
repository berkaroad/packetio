package packetio

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"errors"
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
}

// New is to create PacketIO
func New(conn net.Conn) *PacketIO {
	p := new(PacketIO)
	p.rb = bufio.NewReaderSize(conn, defaultReaderSize)
	p.wb = conn
	p.sequence = 0
	return p
}

// ReadPacket is to read packet.
func (p *PacketIO) ReadPacket() ([]byte, error) {
	p.sequence = 0
	header := []byte{0, 0, 0, 0}
	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, ErrBadConn
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}
	sequence := uint8(header[3])
	if sequence != p.sequence {
		return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.sequence)
	}
	p.sequence++

	total := make([]byte, length)
	if _, err := io.ReadFull(p.rb, total); err != nil {
		return nil, ErrBadConn
	}

	for length == maxPayloadLen {
		header = []byte{0, 0, 0, 0}
		if _, err := io.ReadFull(p.rb, header); err != nil {
			return nil, ErrBadConn
		}
		length = int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		if length < 1 {
			return nil, fmt.Errorf("invalid payload length %d", length)
		}
		sequence = uint8(header[3])
		if sequence != p.sequence {
			return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.sequence)
		}
		p.sequence++

		data := make([]byte, length)
		if _, err := io.ReadFull(p.rb, data); err != nil {
			return nil, ErrBadConn
		}
		total = append(total, data...)
	}
	fmt.Println("ReadPacket", total)
	return total, nil
}

// WritePacket is to write packet.
func (p *PacketIO) WritePacket(data []byte) error {
	p.sequence = 0
	length := len(data)
	for length >= maxPayloadLen {
		buffer := make([]byte, 4, 4+maxPayloadLen)
		buffer[0] = 0xff
		buffer[1] = 0xff
		buffer[2] = 0xff
		buffer[3] = p.sequence

		if n, err := p.wb.Write(append(buffer, data[:maxPayloadLen]...)); err != nil {
			return ErrBadConn
		} else if n != (4 + maxPayloadLen) {
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

	if n, err := p.wb.Write(append(buffer, data...)); err != nil {
		return ErrBadConn
	} else if n != len(data)+4 {
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
	p.sequence = 0
	if data == nil {
		if direct == true {
			n, err := p.wb.Write(total)
			if err != nil {
				return nil, ErrBadConn
			}
			if n != len(total) {
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
			return nil, ErrBadConn
		} else if n != len(total) {
			return nil, ErrBadConn
		}
		if DEBUG {
			consoleLog.Println("WritePacketBatch", total)
		}
	}
	return total, nil
}
