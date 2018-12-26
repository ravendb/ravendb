using System;
using FastTests.Voron;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12506 : StorageTest
    {
        [Fact]
        public void Error_on_db_creation_must_not_cause_failure_on_next_db_load()
        {
            var dataDir = DataDir;

            using (var options = StorageEnvironmentOptions.ForPath(dataDir))
            {
                options.SimulateFailureOnDbCreation = true;

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var a = new StorageEnvironment(options))
                    {

                    }
                });
            }

            using (var options = StorageEnvironmentOptions.ForPath(dataDir))
            {
                using (var s = new StorageEnvironment(options))
                {

                }
            }
        }

        [Fact]
        public void Page_locator_must_not_return_true_on_invalid_page_number()
        {
            using (var tx = Env.WriteTransaction())
            {
                var pageLocator = new PageLocator(tx.LowLevelTransaction);

                Assert.False(pageLocator.TryGetReadOnlyPage(-1, out _));

                Assert.False(pageLocator.TryGetWritablePage(-1, out _));
            }
        }
    }
}
