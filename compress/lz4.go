// +build !no_gzip

package compress

import (
	"bytes"
	"io/ioutil"

	"github.com/pierrec/lz4"
	"github.com/xitongsys/parquet-go/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_LZ4] = &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			lz4Writer := lz4.NewWriter(res)
			lz4Writer.Reset(res)
			lz4Writer.Write(buf)
			lz4Writer.Close()
			lz4Writer.Reset(nil)
			return res.Bytes()
		},
		Uncompress: func(buf []byte) (i []byte, err error) {
			rbuf := bytes.NewReader(buf)
			lz4Reader := lz4.NewReader(rbuf)
			res, err := ioutil.ReadAll(lz4Reader)
			return res, err
		},
	}
}
