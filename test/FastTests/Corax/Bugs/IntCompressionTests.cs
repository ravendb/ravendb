using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Debugging;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Util.PFor;
using Voron.Util.Simd;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs
{
    public unsafe class IntCompressionTests : NoDisposalNeeded
    {
        public IntCompressionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanEncodeAndDecode()
        {
            var data = new[]
            {
                5914189828,
                7482720260,
                8355201028,
                9538097156,
                11308044292,
                18757062660,
                34796146692,
                58804326404,
                67100631044
            };

            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            var buffer = stackalloc byte[1024];
            int sizeUsed;
            fixed (long* l = data.ToArray())
            {
                using var encoder = new FastPForEncoder(allocator);
                encoder.Encode(l, data.Length); 
                (int count, sizeUsed) = encoder.Write(buffer, 1024);
                Assert.Equal(data.Length, count);
            }
            
            
            var output = stackalloc long[256];
            var idx = 0;
            using var reader = new FastPForDecoder(allocator);
            reader.Init(buffer, sizeUsed);
            while (true)
            {
                var read = reader.Read(output, 256);
                if (read == 0)
                    break;
                for (int i = 0; i < read; i++, idx++)
                {
                    Assert.Equal(data[idx], output[i]);
                }
            }
            Assert.Equal(data.Length, idx);
        }

        [RavenMultiplatformFact(RavenTestCategory.Corax | RavenTestCategory.Voron, RavenPlatform.Linux | RavenPlatform.Windows)]
        public void CanEncodeAndDecodeSafely()
        {
            using var stream = typeof(IntCompressionTests).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs.access_violation.json.gz");
            using var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

            var data = JsonConvert.DeserializeObject<long[]>(streamReader.ReadToEnd());
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);

            var bufferSize = 2610;
            var buf = ElectricFencedMemory.Instance.Allocate(bufferSize);
            try
            {
                int sizeUsed;
                fixed (long* l = data.ToArray())
                {
                    using var encoder = new FastPForEncoder(allocator);
                    encoder.Encode(l, data.Length); 
                    (int count, sizeUsed) = encoder.Write(buf, 2610);
                    Assert.Equal(data.Length, count);
                    Assert.Equal(bufferSize, sizeUsed);
                }
                
                var output = stackalloc long[256];
                var idx = 0;
                using var reader = new FastPForDecoder(allocator);
                reader.Init(buf, sizeUsed);
                while (true)
                {
                    var read = reader.Read(output, 256);
                    if (read == 0)
                        break;
                    for (int i = 0; i < read; i++)
                    {
                        Assert.Equal(data[idx++], output[i]);
                    }
                }
                Assert.Equal(data.Length, idx);
            }
            finally
            {
                ElectricFencedMemory.Instance.Free(buf);
            }
        }
    }
}
