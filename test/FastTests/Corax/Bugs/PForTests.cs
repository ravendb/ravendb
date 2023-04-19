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
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs
{
    public unsafe class PForTests : NoDisposalNeeded
    {
        public PForTests(ITestOutputHelper output) : base(output)
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

            Span<byte> buffer = stackalloc byte[1024];
            uint* scratch = stackalloc uint[PForEncoder.BufferLen];
            var encoder = new PForEncoder(buffer, scratch);
            for (int i = 0; i < data.Length; i++)
            {
                Assert.True(encoder.TryAdd(data[i]));
            }
            Assert.True(encoder.TryClose());

            var decoderState = new PForDecoder.DecoderState(encoder.SizeInBytes);
            Span<long> output = stackalloc long[128];
            var idx = 0;

            while (true)
            {
                var read = PForDecoder.Decode(ref decoderState, buffer[..encoder.SizeInBytes], output);
                if (read == 0)
                    break;
                for (int i = 0; i < read; i++, idx++)
                {
                    Assert.Equal(data[idx], output[i]);
                }
            }
            Assert.Equal(data.Length, idx);
        }

        [MultiplatformFact(RavenPlatform.Linux | RavenPlatform.Windows)]
        public void CanEncodeAndDecodeSafely()
        {
            using var stream = typeof(PForTests).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs.access_violation.json.gz");
            using var reader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

            var data = JsonConvert.DeserializeObject<long[]>(reader.ReadToEnd());

            var buf = ElectricFencedMemory.Instance.Allocate(Container.MaxSizeInsideContainerPage);
            try
            {
                var buffer = new Span<byte>(buf, Container.MaxSizeInsideContainerPage);
                var offset = VariableSizeEncoding.Write(buffer, data.Length);
                uint* scratch = stackalloc uint[PForEncoder.BufferLen];
                var encoder = new PForEncoder(buffer[offset..], scratch);
                for (int i = 0; i < data.Length; i++)
                {
                    Assert.True(encoder.TryAdd(data[i]));
                }
                Assert.True(encoder.TryClose());

                var decoderState = new PForDecoder.DecoderState(encoder.SizeInBytes);
                Span<long> output = stackalloc long[128];
                var idx = 0;
                var decode = buffer[offset..(offset + encoder.SizeInBytes)];

                while (true)
                {
                    var read = PForDecoder.Decode(ref decoderState, decode, output);
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
