using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26049(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Compression)]
        public void WillEmitEndFrameForZstd()
        {
            var buffer = "hello world"u8;
            var outputStream = new MemoryStream();
            using (var stream = ZstdStream.Compress(outputStream))
            {
                stream.Write(buffer);
            }
            VerifyEndFrameProperlyWritten(outputStream.ToArray());
        }

        private static unsafe void VerifyEndFrameProperlyWritten(byte[] compressed)
        {
            var outputBuffer = new byte[1024];

            fixed (byte* pBuffer = outputBuffer, pOutput = compressed)
            {
                var ctx = new ZstdLib.CompressContext(0);
                var output = new ZstdLib.ZSTD_outBuffer { Source = pBuffer, Position = UIntPtr.Zero, Size = (UIntPtr)outputBuffer.Length };
                var input = new ZstdLib.ZSTD_inBuffer { Source = pOutput, Position = UIntPtr.Zero, Size = (UIntPtr)compressed.Length };
                var v = ZstdLib.ZSTD_decompressStream(ctx.Decompression, &output, &input);
                Assert.NotEqual(0ul,output.Size);
                ZstdLib.AssertZstdSuccess(v);
                Assert.Equal(0ul, v.ToUInt64());
            }
        }

        [RavenFact(RavenTestCategory.Compression)]
        public async Task WillEmitEndFrameForZstdAsync()
        {
            var buffer = "hello world";
            var outputStream = new MemoryStream();
            await using (var stream = ZstdStream.Compress(outputStream))
            {
                await stream.WriteAsync(Encoding.UTF8.GetBytes(buffer));
            }
            VerifyEndFrameProperlyWritten(outputStream.ToArray());
        }
    }
}
