package diskbuf

import (
	diskBuf "github.com/ShoshinNikita/go-disk-buffer"
)

// https://github.com/ShoshinNikita/go-disk-buffer
// https://github.com/tj/go-disk-buffer

func test() {
	b := diskBuf.NewBufferWithMaxMemorySize(10 * 1024 * 1024)
	b.ChangeTempDir("/data/dbbak/")
	b.Write([]byte("test"))
}
