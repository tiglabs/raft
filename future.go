package raft

type deferError struct {
	errCh chan error
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) respond(err error) {
	d.errCh <- err
	close(d.errCh)
}

func (d *deferError) error() <-chan error {
	return d.errCh
}

type Future struct {
	deferError
	respCh chan interface{}
}

func newFuture() *Future {
	f := &Future{
		respCh: make(chan interface{}, 1),
	}
	f.init()
	return f
}

func (f *Future) respond(resp interface{}, err error) {
	f.deferError.respond(err)
	if err == nil {
		f.respCh <- resp
	}
	close(f.respCh)
}

func (f *Future) Response() (resp interface{}, err error) {
	err = <-f.error()
	if err != nil {
		return
	}
	resp = <-f.respCh
	return
}
