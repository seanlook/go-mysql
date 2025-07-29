package diskbuf

import (
	diskBuf "github.com/ShoshinNikita/go-disk-buffer"
)

// https://github.com/ShoshinNikita/go-disk-buffer
// https://github.com/tj/go-disk-buffer

func test() {
	b := diskBuf.NewBufferWithMaxMemorySize(10 * 1024 * 1024)
	_ = b.ChangeTempDir("/data/dbbak/")
	_, _ = b.Write([]byte("test"))
}
