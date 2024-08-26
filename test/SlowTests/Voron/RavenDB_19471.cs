using System.IO;
using FastTests.Voron;
using Sparrow.Utils;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_19471 : StorageTest
    {
        public RavenDB_19471(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxScratchBufferSize = 64 * 1024 * 4;
        }


        [Fact]
        public void ValidPagesShouldNotChangeOnPageOrChecksumInvalidException()
        {
            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            Configure(options);
            using (var env = new StorageEnvironment(options))
            {
                for (int i = 0; i < 10000; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("items");

                        tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                        tx.Commit();
                    }
                }
                env.FlushLogToDataFile();

                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                {
                    operation.SyncDataFile();
                }
            }

            var op = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)options;
            //corrupt datafile
            using (var fileStream = SafeFileStream.Create(op.FilePath.FullPath,
                       FileMode.Open,
                       FileAccess.ReadWrite,
                       FileShare.ReadWrite | FileShare.Delete))
            {
                var pos = 521000;
                fileStream.Position = pos;

                var buffer = new byte[12];

                var remaining = buffer.Length;
                var start = 0;
                while (remaining > 0)
                {
                    var read = fileStream.Read(buffer, start, remaining);
                    if (read == 0)
                        break;
                    start += read;
                    remaining -= read;
                }

                byte value = 0;
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] != value)
                        buffer[i] = value;
                    else
                        buffer[i] = (byte)(value + 1); // we really want to change the original value here so it must not stay the same
                }
                fileStream.Position = pos;
                fileStream.Write(buffer, 0, buffer.Length);
            }

            options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            Configure(options);
            using (var env = new StorageEnvironment(options))
            {
                Assert.Throws<InvalidDataException>(() =>
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        using (var tx = env.ReadTransaction())
                        {
                            var tree = tx.ReadTree("items");

                            tree.Read("items/" + i);
                        }
                    }
                });
                Assert.Equal(9214364837600034815, env._validPagesAfterLoad[0]);
            }
        }
    }
}
