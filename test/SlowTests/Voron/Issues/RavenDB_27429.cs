using System.Text;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27429 : StorageTest
{
    public RavenDB_27429(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        // every transaction here is large enough to be worth compressing
        options.CompressTxAboveSizeInBytes = 1024;
        options.ManualFlushing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanRecoverAJournalHoldingBothCodecs()
    {
        RequireFileBasedPager();

        // the codec is resolved per transaction, so flipping it between commits lands Lz4 and Zstd
        // entries in the same journal file - which is what a device reclassification, or a pool journal
        // recycled across a codec change, produces at runtime
        var codecs = new[]
        {
            JournalCompressionAlgorithm.Lz4,
            JournalCompressionAlgorithm.Zstd,
            JournalCompressionAlgorithm.Lz4,
            JournalCompressionAlgorithm.Zstd
        };

        for (int i = 0; i < codecs.Length; i++)
        {
            Env.Options.JournalCompressionAlgorithm = codecs[i];

            // an explicit codec is honoured verbatim, so the rounds really do differ - without this the
            // test would pass just as happily if every entry ended up Lz4
            Assert.Equal(codecs[i], Env.Journal.ResolveJournalCompressionAlgorithm());

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("codecs");
                for (int j = 0; j < 256; j++)
                    tree.Add(Key(i, j), Value(i, j));

                tx.Commit();
            }
        }

        RestartDatabase();

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree("codecs");
            Assert.NotNull(tree);

            for (int i = 0; i < codecs.Length; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    var read = tree.Read(Key(i, j));
                    Assert.NotNull(read);

                    var expected = Value(i, j);
                    var actual = new byte[expected.Length];
                    Assert.Equal(expected.Length, read.Reader.Read(actual, 0, actual.Length));
                    Assert.Equal(expected, actual);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AnUnclassifiedDeviceKeepsTheCurrentCodec()
    {
        RequireFileBasedPager();

        // Auto is the default, and nothing here writes enough to classify the device - the resolution
        // has to stay on Lz4 rather than guess, and the data has to come back either way
        Assert.Equal(JournalCompressionAlgorithm.Auto, Env.Options.JournalCompressionAlgorithm);
        Assert.Equal(JournalCompressionAlgorithm.Lz4, Env.Journal.ResolveJournalCompressionAlgorithm());

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("auto");
            for (int j = 0; j < 256; j++)
                tree.Add(Key(0, j), Value(0, j));

            tx.Commit();
        }

        RestartDatabase();

        using (var tx = Env.ReadTransaction())
        {
            var tree = tx.ReadTree("auto");
            Assert.NotNull(tree);

            for (int j = 0; j < 256; j++)
                Assert.NotNull(tree.Read(Key(0, j)));
        }
    }

    private static string Key(int round, int index) => $"codec/{round:D2}/{index:D4}";

    // compressible on purpose, so the codec choice is actually exercised
    private static byte[] Value(int round, int index) =>
        Encoding.UTF8.GetBytes(new string((char)('a' + ((round + index) % 26)), 512));
}
