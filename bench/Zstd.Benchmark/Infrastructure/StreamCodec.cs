using System;
using System.IO;
using System.IO.Compression;
using Sparrow.Utils;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Synchronous helpers over the production <see cref="ZstdStream"/>, for setup and reports.
    /// </summary>
    internal static class StreamCodec
    {
        public static byte[] Compress(ReadOnlySpan<byte> payload, CompressionLevel level, int chunkSize, Stream counter = null)
        {
            using MemoryStream output = new();
            Stream target = counter ?? output;
            using (ZstdStream zstd = ZstdStream.Compress(target, level, leaveOpen: true))
            {
                for (int offset = 0; offset < payload.Length; offset += chunkSize)
                    zstd.Write(payload.Slice(offset, Math.Min(chunkSize, payload.Length - offset)));
            }

            return counter == null ? output.ToArray() : null;
        }

        public static byte[] Decompress(Stream source, int chunkSize, int expectedSize = 0)
        {
            using MemoryStream output = new(expectedSize);
            byte[] buffer = new byte[chunkSize];
            using ZstdStream zstd = ZstdStream.Decompress(source, leaveOpen: true);
            int read;
            while ((read = zstd.Read(buffer)) != 0)
                output.Write(buffer, 0, read);
            return output.ToArray();
        }

        public static void VerifyRoundTrip(byte[] compressed, byte[] expected)
        {
            byte[] decompressed = Decompress(new MemoryStream(compressed, writable: false), 32 * 1024, expected.Length);
            if (decompressed.AsSpan().SequenceEqual(expected) == false)
                throw new InvalidOperationException($"Stream round trip failed ({decompressed.Length} bytes, expected {expected.Length})");
        }
    }
}
