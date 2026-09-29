using System;
using System.Collections.Generic;
using System.Text;
using FastTests.Voron;
using Raven.Client.Documents.Indexes;
using Raven.Server.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27509 : StorageTest
    {
        public RavenDB_27509(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly long[] Values =
        {
            0,
            1,
            127,
            128,
            16_383,
            16_384,
            (1L << 28) - 1,
            1L << 28,
            int.MaxValue,          // 5 bytes, still fits in an int
            1L << 31,              // 5 bytes, does not fit in an int anymore
            (1L << 31) + 12_345,
            1L << 32,
            (1L << 33) + 7,
            1L << 35,              // 6 bytes -> shift 35 wraps to 3 with an int accumulator
            1L << 40,
            (1L << 42) + 99,
            long.MaxValue >> 1,
        };

        [RavenTheory(RavenTestCategory.Lucene | RavenTestCategory.Querying)]
        [InlineData(LuceneIndexInputType.Standard)]
        [InlineData(LuceneIndexInputType.Buffered)]
        public void ReadVLong_must_round_trip_values_larger_than_int32(LuceneIndexInputType inputType)
        {
            using (var tx = Env.WriteTransaction())
            using (var cache = new TempFileCache(Env.Options))
            {
                var dir = new LuceneVoronDirectory(tx, Env, cache, inputType);
                var state = new VoronState(tx);

                using (var output = dir.CreateOutput("file", state))
                {
                    foreach (var value in Values)
                        output.WriteVLong(value);

                    // padding so that the buffered fast path (needs 10 more bytes in the buffer) is taken for all values
                    for (var i = 0; i < 64; i++)
                        output.WriteByte(0);
                }

                var failures = new List<string>();

                using (var input = dir.OpenInput("file", state))
                {
                    foreach (var expected in Values)
                    {
                        var actual = input.ReadVLong(state);
                        if (actual != expected)
                            failures.Add($"expected {expected} (0x{expected:X}) but read {actual} (0x{actual:X})");
                    }
                }

                Assert.True(failures.Count == 0, $"{inputType}: {failures.Count} VLong values were decoded wrongly:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
            }
        }

        [RavenFact(RavenTestCategory.Lucene | RavenTestCategory.Querying)]
        public void Buffered_and_standard_inputs_must_decode_the_same_bytes_identically()
        {
            using (var tx = Env.WriteTransaction())
            using (var cache = new TempFileCache(Env.Options))
            {
                var standardDir = new LuceneVoronDirectory(tx, Env, cache, "Standard", LuceneIndexInputType.Standard);
                var bufferedDir = new LuceneVoronDirectory(tx, Env, cache, "Buffered", LuceneIndexInputType.Buffered);
                var state = new VoronState(tx);

                // the same term-dictionary-like sequence in both directories: a small VInt, a VLong delta, another VLong delta
                var random = new Random(1234);
                var deltas = new List<(int docFreq, long freqDelta, long proxDelta)>();
                for (var i = 0; i < 2_000; i++)
                {
                    var freqDelta = i % 97 == 0 ? (1L << 31) + random.Next() : random.Next(1, 1 << 20);
                    var proxDelta = i % 131 == 0 ? (1L << 33) + random.Next() : random.Next(1, 1 << 20);
                    deltas.Add((random.Next(1, 1 << 15), freqDelta, proxDelta));
                }

                foreach (var dir in new[] { standardDir, bufferedDir })
                {
                    using (var output = dir.CreateOutput("_0.tis", state))
                    {
                        foreach (var (docFreq, freqDelta, proxDelta) in deltas)
                        {
                            output.WriteVInt(docFreq);
                            output.WriteVLong(freqDelta);
                            output.WriteVLong(proxDelta);
                        }
                    }
                }

                var mismatches = new StringBuilder();

                using (var standard = standardDir.OpenInput("_0.tis", state))
                using (var buffered = bufferedDir.OpenInput("_0.tis", state))
                {
                    long standardFreq = 0, bufferedFreq = 0, standardProx = 0, bufferedProx = 0;

                    for (var i = 0; i < deltas.Count; i++)
                    {
                        var standardDocFreq = standard.ReadVInt(state);
                        var bufferedDocFreq = buffered.ReadVInt(state);

                        standardFreq += standard.ReadVLong(state);
                        bufferedFreq += buffered.ReadVLong(state);

                        standardProx += standard.ReadVLong(state);
                        bufferedProx += buffered.ReadVLong(state);

                        if (standardDocFreq != bufferedDocFreq || standardFreq != bufferedFreq || standardProx != bufferedProx)
                        {
                            mismatches.AppendLine($"entry {i}: docFreq {standardDocFreq}/{bufferedDocFreq}, freqPointer {standardFreq}/{bufferedFreq}, proxPointer {standardProx}/{bufferedProx}");
                            if (mismatches.Length > 4_000)
                                break;
                        }
                    }
                }

                Assert.True(mismatches.Length == 0, $"Buffered input diverged from Standard input (standard/buffered):{Environment.NewLine}{mismatches}");
            }
        }
    }
}
