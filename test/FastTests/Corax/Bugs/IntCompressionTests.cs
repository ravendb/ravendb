using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Sparrow.Compression;
using Sparrow.Server.Debugging;
using Tests.Infrastructure;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
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

            var buffer = stackalloc byte[1024];
            int sizeUsed;
            fixed (long* l = data.ToArray())
            {
                (int count, sizeUsed) = SimdBitPacker<SortedDifferentials>.Encode(l, data.Length, buffer, 1024);
                Assert.Equal(data.Length, count);
            }
            
            
            var output = stackalloc long[256];
            var idx = 0;
            var reader = new SimdBitPacker<SortedDifferentials>.Reader(buffer, sizeUsed);
            while (true)
            {
                var read = reader.Fill(output, 256);
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

            var bufferSize = 4238;
            var buf = ElectricFencedMemory.Instance.Allocate(bufferSize);
            try
            {
                int sizeUsed;
                fixed (long* l = data.ToArray())
                {
                    (int count,  sizeUsed) = SimdBitPacker<SortedDifferentials>.Encode(l, data.Length, buf, 4238);
                    Assert.Equal(data.Length, count);
                    Assert.Equal(bufferSize, sizeUsed);
                }
                
                var output = stackalloc long[256];
                var idx = 0;
                var reader = new SimdBitPacker<SortedDifferentials>.Reader(buf, sizeUsed);
                while (true)
                {
                    var read = reader.Fill(output, 256);
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
