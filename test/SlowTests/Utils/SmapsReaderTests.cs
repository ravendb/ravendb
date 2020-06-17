using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using FastTests;
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

        [Fact]
        public void ParsesSmapsProperly()
        {
            var assembly = typeof(SmapsReaderTests).Assembly;
            var smapsReader = new SmapsReader(new[]
            {
                new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize]
            });

            SmapsReader.SmapsReadResult<SmapsTestResult> result;
            
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
                using (StreamReader reader = new StreamReader(_smapsSnapshotStream))
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
