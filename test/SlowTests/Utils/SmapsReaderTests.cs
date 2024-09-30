using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using FastTests;
using Sparrow;
using Sparrow.Platform.Posix;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Utils
{
    public class SmapsReaderTests : NoDisposalNeeded
    {
        public SmapsReaderTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string SmapsRollup = @"605f1bf67000-7fff4b6e2000 ---p 00000000 00:00 0                          [rollup]
Rss:              843564 kB
Pss:              809811 kB
Pss_Dirty:        543903 kB
Pss_Anon:         422104 kB
Pss_File:         265771 kB
Pss_Shmem:        121935 kB
Shared_Clean:      61696 kB
Shared_Dirty:         32 kB
Private_Clean:    237944 kB
Private_Dirty:    543892 kB
Referenced:       713176 kB
Anonymous:        422104 kB
LazyFree:              0 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
Shared_Hugetlb:        0 kB
Private_Hugetlb:       0 kB
Swap:                  0 kB
SwapPss:               0 kB
Locked:                0 kB
";


        [Fact]
        public void ParsesSmapsProperlyFromRollup()
        {
            var smapsReader = SmapsRollupReader.CreateNew([new byte[AbstractSmapsReader.BufferSize], new byte[AbstractSmapsReader.BufferSize]]);
            AbstractSmapsReader.SmapsReadResult<SmapsTestResult> result;
            using (var smapsStream = new FakeProcSmapsEntriesStream(new MemoryStream(Encoding.UTF8.GetBytes(SmapsRollup))))
            {
                result = smapsReader
                    .CalculateMemUsageFromSmaps<SmapsTestResult>(smapsStream, pid: 1234);
            }

            Assert.Single(result.SmapsResults.Entries);

            var totalDirty = new Size(0, SizeUnit.Bytes);
            totalDirty.Add(543892, SizeUnit.Kilobytes);
            totalDirty.Add(32, SizeUnit.Kilobytes);

            Assert.Equal(new Size(843564, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes), result.Rss);
            Assert.Equal(new Size(61696, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes), result.SharedClean);
            Assert.Equal(new Size(237944, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes), result.PrivateClean);
            Assert.Equal(new Size(0, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes), result.Swap);
            Assert.Equal(totalDirty.GetValue(SizeUnit.Bytes), result.TotalDirty);
        }

        [Fact]
        public void ParsesSmapsProperly()
        {
            var assembly = typeof(SmapsReaderTests).Assembly;
            var smapsReader = SmapsReader.CreateNew([new byte[AbstractSmapsReader.BufferSize], new byte[AbstractSmapsReader.BufferSize]]);

            AbstractSmapsReader.SmapsReadResult<SmapsTestResult> result;

            using (var fs =
                assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_15159.12119.smaps.gz"))
            using (var deflated = new GZipStream(fs, CompressionMode.Decompress))
            using (var smapsStream = new FakeProcSmapsEntriesStream(deflated))
            {
                result = smapsReader
                    .CalculateMemUsageFromSmaps<SmapsTestResult>(smapsStream, pid: 1234);
            }

            // 385 .buffers
            // 181 .voron
            Assert.Equal(385 + 181, result.SmapsResults.Entries.Count);

            // cat 12119.smaps | grep -e "rw-s" -A 3 | awk '$1 ~ /Rss/ {sum += $2} END {print sum}'
            Assert.Equal(722136L * 1024, result.Rss);

            // cat 12119.smaps | grep -e "rw-s" -A 16 | awk '$1 ~ /Swap/ {sum += $2} END {print sum}'
            Assert.Equal(1348L * 1024, result.Swap);
        }

        private struct SmapsTestResult : ISmapsReaderResultAction
        {
            public List<SmapsReaderResults> Entries;
            public void Add(SmapsReaderResults results)
            {
                if (Entries == null)
                    Entries = new List<SmapsReaderResults>();

                Entries.Add(results);
            }
        }

        private class FakeProcSmapsEntriesStream : Stream
        {
            private readonly Stream _smapsSnapshotStream;

            private readonly IEnumerator<string> _entriesEnumerator;

            public FakeProcSmapsEntriesStream(Stream smapsSnapshotStream)
            {
                _smapsSnapshotStream = smapsSnapshotStream;
                _entriesEnumerator = ReadEntry().GetEnumerator();
            }

            private IEnumerable<string> ReadEntry()
            {
                using (StreamReader reader = new(_smapsSnapshotStream))
                {
                    var currentEntry = new StringBuilder();
                    string line;

                    while ((line = reader.ReadLine()) != null)
                    {
                        currentEntry.Append(line + '\n');

                        if (line.StartsWith("VmFlags"))
                        {
                            yield return currentEntry.ToString();
                            currentEntry = new StringBuilder();
                        }
                    }

                    yield return currentEntry.ToString();
                }
            }

            public override void Flush()
            {
                throw new System.NotImplementedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (_entriesEnumerator.MoveNext() == false)
                    return 0;

                var currentEntryString = _entriesEnumerator.Current;
                var entryBytes = Encoding.UTF8.GetBytes(currentEntryString);
                entryBytes.CopyTo(new Memory<byte>(buffer));
                return entryBytes.Length;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new System.NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new System.NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new System.NotImplementedException();
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => 0;
            public override long Position { get; set; }
        }

    }
}
