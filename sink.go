//@Author: springliao
//@Description:
//@Time: 2021/11/17 12:22

package filebeat

// Sink
type Sink interface {

	// OnMessage
	//  @param msg
	OnMessage(msg string)
}
