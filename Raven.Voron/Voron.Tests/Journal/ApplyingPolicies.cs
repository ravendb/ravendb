using Xunit;

namespace Voron.Tests.Journal
{
    public class ApplyingPolicies : StorageTest
    {
	    protected override void Configure(StorageEnvironmentOptions options)
	    {
		    options.ManualFlushing = true;
	    }

	    [Fact]
        public void DbStartUpRequiresFlushing()
        {
            Assert.True(Env.Journal.SizeOfUnflushedTransactionsInJournalFile() > 0);
        }

        [Fact]
        public void AfterFlushingThereIsNothingToFlush()
        {
            Assert.NotNull(Env.Journal.CurrentFile);
            Env.FlushLogToDataFile();
            Assert.NotNull(Env.Journal.CurrentFile);
            Assert.False(Env.Journal.SizeOfUnflushedTransactionsInJournalFile() == 0);
        }
    }
}