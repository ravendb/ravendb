using Raven.Server.Config;
using Sparrow;
using Voron;

namespace Raven.Server.Utils
{
    public static class VoronOptionsFromConfiguration
    {
        public static void Apply(StorageEnvironmentOptions options, RavenConfiguration configuration)
        {
            // Must be assigned first: this setter overwrites MaxLogFileSize, MaxScratchBufferSize and
            // MaxNumberOfPagesInJournalBeforeFlush, and while it is on, the MaxScratchBufferSize setter
            // clamps downward.
            options.ForceUsing32BitsPager = configuration.Storage.ForceUsing32BitsPager;

            options.CompressTxAboveSizeInBytes = configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.EnablePrefetching = configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = configuration.Storage.DiscardVirtualMemory;
            options.UseSequentialReadAheadHintForJournalRecovery = configuration.Storage.UseSequentialReadAheadHintForJournalRecovery;
            options.TimeToSyncAfterFlushInSec = (int)configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = configuration.Storage.SyncJournalsCountThreshold;
            options.MaxUnsyncedBytesBeforeSync = configuration.Storage.MaxUnsyncedSizeBeforeSync.GetValue(SizeUnit.Bytes);
            options.MaxUnsyncedBytesBeforeMandatorySync = configuration.Storage.MaxUnsyncedSizeBeforeMandatorySync.GetValue(SizeUnit.Bytes);
            options.MaxConcurrentJournalWrites = configuration.Storage.MaxConcurrentJournalWrites;
            options.PipelineJournalWritesAboveLatencyInTicks = configuration.Storage.PipelineJournalWritesAboveLatencyInTicks;
            options.ConsolidationTargetWriteSizeInBytes = configuration.Storage.ConsolidationTargetWriteSize.GetValue(SizeUnit.Bytes);
            options.SyncWritebackBlockSizeInMb = configuration.Storage.SyncWritebackBlockSizeInMb;
            options.SyncWritebackMinContiguousSizeInKb = configuration.Storage.SyncWritebackMinContiguousSizeInKb;
            options.SyncWritebackBarrierCostThresholdInMs = configuration.Storage.SyncWritebackBarrierCostThresholdInMs;
            options.SyncWritebackDrainQueueDepthThreshold = configuration.Storage.SyncWritebackDrainQueueDepthThreshold;
            options.IgnoreInvalidJournalErrors = configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
            options.MaxNumberOfRecyclableJournals = configuration.Storage.MaxNumberOfRecyclableJournals;
            options.DisableSparseRegions = configuration.Storage.DisableSparseRegions;
            options.JournalsCompressionAcceleration = configuration.Storage.JournalsCompressionAcceleration;
            options.JournalCompressionAlgorithm = configuration.Storage.JournalsCompressionAlgorithm;
            options.MinimumSharedJournalsMergeCount = configuration.Indexing.MinimumSharedJournalsMergeCount;
            options.MaxLogFileSize = configuration.Storage.MaxJournalFileSize.GetValue(SizeUnit.Bytes);
        }
    }
}
