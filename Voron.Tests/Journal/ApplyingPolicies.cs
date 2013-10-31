using Xunit;

namespace Voron.Tests.Journal
{
    public class ApplyingPolicies : StorageTest
    {
        [Fact]
        public void DbStartUpRequiresFlushing()
        {
            Assert.True(Env.Journal.HasTransactionsToFlush());
        }

        [Fact]
        public void AfterFlushingThereIsNothingToFlush()
        {
            Env.FlushLogToDataFile();
            Assert.NotNull(Env.Journal.CurrentFile);
            Assert.False(Env.Journal.HasTransactionsToFlush());
        }
    }
}