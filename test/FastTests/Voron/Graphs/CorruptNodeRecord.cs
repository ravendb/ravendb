using System;
using System.IO;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Voron.Graphs;

public class CorruptNodeRecord(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodeRecordDeclaringMoreLevelsThanItHasBytesIsRejected()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 1, countOfLevels = 2^25 — in a 6-byte record.
            var corrupt = new byte[] { 0x01, 0x01, 0x80, 0x80, 0x80, 0x10 };

            Assert.Throws<InvalidDataException>(() => Hnsw.Node.Decode(txw.LowLevelTransaction, corrupt));
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodeRecordTruncatedInsideVarintIsRejected()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 1, then a countOfLevels varint that never terminates.
            var truncated = new byte[] { 0x01, 0x01, 0x80 };

            Assert.Throws<InvalidDataException>(() => Hnsw.Node.Decode(txw.LowLevelTransaction, truncated));
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EdgeCountLargerThanRecordIsRejected()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 1, countOfLevels = 1, then an edge count of 100 with no edges behind it.
            var corrupt = new byte[] { 0x01, 0x01, 0x01, 0x64 };

            Assert.Throws<InvalidDataException>(() =>
            {
                var reader = Hnsw.Node.Decode(txw.LowLevelTransaction, corrupt);
                var node = new Hnsw.Node();
                reader.LoadInto(ref node);
            });
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodeRecordHoldingMoreLevelsThanItDeclaresIsRejected()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 1, countOfLevels = 1 — followed by four zero-edge levels.
            var corrupt = new byte[] { 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00 };

            Assert.Throws<InvalidDataException>(() =>
            {
                var reader = Hnsw.Node.Decode(txw.LowLevelTransaction, corrupt);
                var node = new Hnsw.Node();
                reader.LoadInto(ref node);
            });
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void NodeRecordHoldingFewerLevelsThanItDeclaresIsRejected()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 1, countOfLevels = 3. The three remaining bytes
            // hold a single level (2 edges: 1, +1). The count survives Decode. Only the
            // level tally can catch it.
            var corrupt = new byte[] { 0x01, 0x01, 0x03, 0x02, 0x01, 0x01 };

            Assert.Throws<InvalidDataException>(() =>
            {
                var reader = Hnsw.Node.Decode(txw.LowLevelTransaction, corrupt);
                var node = new Hnsw.Node();
                reader.LoadInto(ref node);
            });
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ValidNodeRecordStillRoundTrips()
    {
        using (var txw = Env.WriteTransaction())
        {
            // postingListId = 1, vectorId = 2, one level holding edges {3, 5} (delta-encoded 3, +2).
            var valid = new byte[] { 0x01, 0x02, 0x01, 0x02, 0x03, 0x02 };

            var reader = Hnsw.Node.Decode(txw.LowLevelTransaction, valid);
            Assert.Equal(1, reader.PostingListId);
            Assert.Equal(2, reader.VectorId);
            Assert.Equal(1, reader.CountOfLevels);

            var node = new Hnsw.Node();
            reader.LoadInto(ref node);
            Assert.Equal(1, node.EdgesPerLevel.Count);
            Assert.Equal(2, node.EdgesPerLevel[0].Count);
            Assert.Equal(3, node.EdgesPerLevel[0][0]);
            Assert.Equal(5, node.EdgesPerLevel[0][1]);
        }
    }
}
