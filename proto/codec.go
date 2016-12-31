package proto

func (e *Entry) Size() uint64 {
	return uint64(17 + len(e.Data))
}

func (c *ConfChange) Size() uint64 {
	return 0
}

func (c *ConfChange) UnMarshal(datas []byte) {
}

func (c *ConfChange) Marshal(datas []byte) {

}
