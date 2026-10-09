using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class ZstdTests : NoDisposalNeeded
    {
        public ZstdTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Compression)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(1024)]
        [InlineData(300 * 1024)]
        [InlineData(3 * 1024 * 1024)]
        public async Task StreamRoundTrip(int size)
        {
            var payload = Payload(size, seed: size);

            // odd chunk sizes, so writes and reads straddle the stream's internal buffer boundaries
            Assert.Equal(payload, Decompress(Compress(payload, CompressionLevel.Optimal, chunkSize: 7919)));
            Assert.Equal(payload, await DecompressAsync(await CompressAsync(payload, CompressionLevel.Fastest, chunkSize: 65521)));
        }

        [RavenFact(RavenTestCategory.Compression)]
        public void FlushMakesEverythingWrittenSoFarReadable()
        {
            var payload = Payload(1024 * 1024, seed: 1);
            var inner = new MemoryStream();
            using var zstd = ZstdStream.Compress(inner, CompressionLevel.Optimal, leaveOpen: true);

            var written = 0;
            foreach (var part in new[] { 1, 1000, 200_000, 300_000 })
            {
                zstd.Write(payload, written, part);
                written += part;
                zstd.Flush();

                // the frame isn't finished, but everything written before the flush has to be decodable by the other side
                Assert.Equal(payload.AsSpan(0, written).ToArray(), Decompress(inner.ToArray()));
            }
        }

        [RavenTheory(RavenTestCategory.Compression)]
        [InlineData(1)]
        [InlineData(4)]
        public void ParallelCompressionRoundTrip(int workers)
        {
            // long enough for zstd to split it into several jobs
            var payload = Payload(12 * 1024 * 1024, seed: 2);
            var inner = new MemoryStream();
            int used;
            using (var zstd = ZstdStream.Compress(inner, CompressionLevel.Fastest, leaveOpen: true, workers))
            {
                for (var offset = 0; offset < payload.Length; offset += 32 * 1024)
                {
                    zstd.Write(payload, offset, Math.Min(32 * 1024, payload.Length - offset));
                    if (offset % (4 * 1024 * 1024) == 0)
                        zstd.Flush();
                }

                used = zstd.Workers;
            }

            // a single threaded native library compresses on the calling thread
            Assert.True(used == 0 || used == workers, $"Expected 0 or {workers} workers, got {used}");
            Assert.Equal(payload, Decompress(inner.ToArray()));
        }

        [RavenFact(RavenTestCategory.Compression)]
        public void PooledContextsDoNotCarryStateBetweenStreams()
        {
            var payload = Payload(512 * 1024, seed: 3);
            var expectedOptimal = Compress(payload, CompressionLevel.Optimal);
            var expectedFastest = Compress(payload, CompressionLevel.Fastest);
            Assert.NotEqual(expectedOptimal, expectedFastest);

            for (var i = 0; i < ZstdLib.ContextPool.MaxPooledContexts + 4; i++)
            {
                // a compression stream abandoned mid-frame gives its context back with an unfinished frame
                var abandoned = ZstdStream.Compress(new MemoryStream(), CompressionLevel.Fastest, leaveOpen: true);
                abandoned.Write(payload, 0, 100_000);
                abandoned.DisposeWithoutFlushing();

                // and a decompression stream that stopped reading in the middle of a frame
                using (var partial = ZstdStream.Decompress(new MemoryStream(expectedOptimal, 0, expectedOptimal.Length * 3 / 4)))
                    partial.ReadExactly(new byte[1000]);

                // zstd output is deterministic, so any parameter or frame state leaking from a previous owner changes the bytes
                Assert.Equal(expectedOptimal, Compress(payload, CompressionLevel.Optimal));
                Assert.Equal(expectedFastest, Compress(payload, CompressionLevel.Fastest));
                Assert.Equal(payload, Decompress(expectedOptimal));

                if (i == 5)
                    ZstdLib.ContextPool.Instance.LowMemory(LowMemorySeverity.ExtremelyLow);
            }

            // a high level context grows too large to be kept, it must still work and leave the pool usable
            Assert.Equal(payload, Decompress(Compress(payload, CompressionLevel.SmallestSize)));
            Assert.Equal(expectedOptimal, Compress(payload, CompressionLevel.Optimal));
        }

        [RavenFact(RavenTestCategory.Compression)]
        public void SmallestSizeDoesNotUseUltraLevels()
        {
            // zstd levels 20-22 need hundreds of MB per compression context (GBs with workers) and a 128MB window to decompress
            var payload = Payload(1024 * 1024, seed: 5);
            var compressed = Compress(payload, CompressionLevel.SmallestSize);

            // frame header (RFC 8878 3.1.1.1): magic, descriptor, then the window descriptor unless the frame is single segment
            var descriptor = compressed[4];
            Assert.Equal(0, (descriptor >> 5) & 1);
            var windowLog = 10 + (compressed[5] >> 3);
            Assert.True(windowLog <= 23, $"Window log {windowLog} means an ultra level (> 19) was used");

            Assert.Equal(payload, Decompress(compressed));
        }

        [RavenFact(RavenTestCategory.Compression)]
        public void ConcurrentStreamsShareThePool()
        {
            var payload = Payload(256 * 1024, seed: 4);
            var expected = Compress(payload, CompressionLevel.Optimal);

            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 8 }, _ =>
            {
                Assert.Equal(expected, Compress(payload, CompressionLevel.Optimal));
                Assert.Equal(payload, Decompress(expected));
            });
        }

        [RavenFact(RavenTestCategory.Compression)]
        public unsafe void DictionaryCompressedValuesDoNotCarryTheDictionaryId()
        {
            var documents = new List<byte[]>();
            for (var i = 0; i < 300; i++)
                documents.Add(Document(i));

            using var dictionary = TrainDictionary(documents.GetRange(0, 200));
            using var oldFormatContext = new ZstdLib.CompressContext(level: 0);

            foreach (var document in documents.GetRange(200, 100))
            {
                var output = new byte[ZstdLib.GetMaxCompression(document.Length) + 32];
                var oldFormat = new byte[output.Length];
                int size, oldFormatSize;
                fixed (byte* src = document)
                fixed (byte* dst = output)
                fixed (byte* oldDst = oldFormat)
                {
                    size = ZstdLib.Compress(src, document.Length, dst, output.Length, dictionary);

                    // what ZstdLib.Compress wrote before: the frame header carries the dictionary id
                    var result = ZstdLib.ZSTD_compress_usingCDict(oldFormatContext.Compression, oldDst, (UIntPtr)oldFormat.Length, src, (UIntPtr)document.Length, dictionary.Compression);
                    ZstdLib.AssertZstdSuccess(result);
                    oldFormatSize = (int)result;
                }

                // frame header descriptor (RFC 8878 3.1.1.1.1): the lowest two bits are the dictionary id field size
                Assert.Equal(0, output[4] & 3);
                Assert.NotEqual(0, oldFormat[4] & 3);
                Assert.Equal(oldFormatSize - DictionaryIdFieldSize(oldFormat[4]), size);

                Assert.Equal(document.Length, ZstdLib.GetDecompressedSize(output.AsSpan(0, size)));
                Assert.Equal(document, DecompressValue(output.AsSpan(0, size), document.Length, dictionary));

                // values written by previous versions stay readable
                Assert.Equal(document, DecompressValue(oldFormat.AsSpan(0, oldFormatSize), document.Length, dictionary));
            }

            // the same thread-static context then compresses without a dictionary
            var plain = documents[0];
            var plainOutput = new byte[ZstdLib.GetMaxCompression(plain.Length)];
            int plainSize;
            fixed (byte* src = plain)
            fixed (byte* dst = plainOutput)
                plainSize = ZstdLib.Compress(src, plain.Length, dst, plainOutput.Length, null);
            Assert.Equal(plain, DecompressValue(plainOutput.AsSpan(0, plainSize), plain.Length, null));
        }

        private static int DictionaryIdFieldSize(byte frameHeaderDescriptor) => (frameHeaderDescriptor & 3) switch
        {
            0 => 0,
            1 => 1,
            2 => 2,
            _ => 4
        };

        private static unsafe ZstdLib.CompressionDictionary TrainDictionary(List<byte[]> samples)
        {
            var total = 0;
            foreach (var sample in samples)
                total += sample.Length;

            var buffer = new byte[total];
            var sizes = new UIntPtr[samples.Count];
            var offset = 0;
            for (var i = 0; i < samples.Count; i++)
            {
                samples[i].CopyTo(buffer, offset);
                sizes[i] = (UIntPtr)samples[i].Length;
                offset += samples[i].Length;
            }

            var dictionaryBuffer = new byte[8 * 1024];
            Span<byte> dictionary = dictionaryBuffer;
            ZstdLib.Train(buffer, sizes, ref dictionary);

            fixed (byte* p = dictionary)
                return new ZstdLib.CompressionDictionary(1, p, dictionary.Length, 3);
        }

        private static byte[] DecompressValue(ReadOnlySpan<byte> compressed, int size, ZstdLib.CompressionDictionary dictionary)
        {
            var output = new byte[size];
            Assert.Equal(size, ZstdLib.Decompress(compressed, output, dictionary));
            return output;
        }

        private static byte[] Document(int i)
        {
            var random = new Random(i);
            string[] cities = { "London", "Berlin", "Madrid", "Warsaw", "Seattle", "Tokyo" };
            var sb = new StringBuilder();
            sb.Append($"{{\"Name\":\"Company {random.Next(100_000)}\",\"Contact\":{{\"Name\":\"Person {random.Next(1000)}\",\"Title\":\"Sales Representative\"}},");
            sb.Append($"\"Address\":{{\"City\":\"{cities[random.Next(cities.Length)]}\",\"PostalCode\":\"{random.Next(10_000, 99_999)}\",\"Country\":\"Country {random.Next(20)}\"}},");
            sb.Append($"\"Phone\":\"({random.Next(100, 999)}) {random.Next(100, 999)}-{random.Next(1000, 9999)}\",\"Orders\":[");
            for (var j = random.Next(1, 6); j > 0; j--)
                sb.Append($"{{\"Product\":\"products/{random.Next(77)}-A\",\"Quantity\":{random.Next(1, 50)},\"PricePerUnit\":{random.Next(1, 300)}.{random.Next(100):00}}},");
            sb.Append("{}]}");
            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Half compressible text, half random bytes, so compressed output spans several of the stream's internal buffers.
        /// </summary>
        private static byte[] Payload(int size, int seed)
        {
            var random = new Random(seed);
            var payload = new byte[size];
            var text = Encoding.UTF8.GetBytes("{\"Name\":\"Company\",\"Address\":{\"City\":\"London\",\"Country\":\"UK\"},\"Lines\":[1,2,3]},");
            for (var offset = 0; offset < size; offset += 4096)
            {
                var block = payload.AsSpan(offset, Math.Min(4096, size - offset));
                if ((offset / 4096) % 2 == 0)
                {
                    random.NextBytes(block);
                    continue;
                }

                for (var i = 0; i < block.Length; i++)
                    block[i] = text[(offset + i) % text.Length];
            }

            return payload;
        }

        private static byte[] Compress(byte[] payload, CompressionLevel level, int chunkSize = 32 * 1024)
        {
            var output = new MemoryStream();
            using (var zstd = ZstdStream.Compress(output, level, leaveOpen: true))
            {
                for (var offset = 0; offset < payload.Length; offset += chunkSize)
                    zstd.Write(payload, offset, Math.Min(chunkSize, payload.Length - offset));
            }

            return output.ToArray();
        }

        private static async Task<byte[]> CompressAsync(byte[] payload, CompressionLevel level, int chunkSize)
        {
            var output = new MemoryStream();
            await using (var zstd = ZstdStream.Compress(output, level, leaveOpen: true))
            {
                for (var offset = 0; offset < payload.Length; offset += chunkSize)
                    await zstd.WriteAsync(payload.AsMemory(offset, Math.Min(chunkSize, payload.Length - offset)));
            }

            return output.ToArray();
        }

        private static byte[] Decompress(byte[] compressed)
        {
            var output = new MemoryStream();
            using (var zstd = ZstdStream.Decompress(new MemoryStream(compressed)))
                zstd.CopyTo(output, 4093);
            return output.ToArray();
        }

        private static async Task<byte[]> DecompressAsync(byte[] compressed)
        {
            var output = new MemoryStream();
            await using (var zstd = ZstdStream.Decompress(new MemoryStream(compressed)))
                await zstd.CopyToAsync(output, 65519);
            return output.ToArray();
        }
    }
}
