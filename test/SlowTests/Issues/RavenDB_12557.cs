using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12557 : StorageTest
    {
        [Fact64Bit]
        public void ShouldNotAllowToIncreaseFileSizeWhenUsingCopyOnWriteMode()
        {
            // we must not increase file size during the recovery process because we create new MMF view but we don't see the already applied changes
            // to data file due to CopyOnWriteMode usage
            // in Voron.Recovery we catch this error and set the file size until we have the right size and don't need to change it during the recovery on db load

            using (var options = StorageEnvironmentOptions.ForPath(DataDir))
            {
                options.ManualFlushing = true;

                using (var env = new StorageEnvironment(options))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        using (var tx = env.WriteTransaction())
                        {
                            var tree = tx.CreateTree("test");

                            tree.Add($"items/{i}/" + new string('a', 2000), new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0});

                            tx.Commit();
                        }
                    }
                }
            }

            Assert.Throws<IncreasingDataFileInCopyOnWriteModeException>(() =>
            {
                using (var options = StorageEnvironmentOptions.ForPath(DataDir))
                {
                    options.ManualFlushing = true;
                    options.CopyOnWriteMode = true;

                    using (var env = new StorageEnvironment(options))
                    {

                    }
                }
            });
        }
    }
}
