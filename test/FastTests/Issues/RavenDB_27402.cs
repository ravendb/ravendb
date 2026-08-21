using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27402 : NoDisposalNeeded
    {
        public RavenDB_27402(ITestOutputHelper output) : base(output)
        {
        }

        // Every member of this set is assigned by VoronOptionsFromConfiguration.Apply.
        private static readonly HashSet<string> ConfigurationDriven = new()
        {
            nameof(StorageEnvironmentOptions.ForceUsing32BitsPager),
            nameof(StorageEnvironmentOptions.CompressTxAboveSizeInBytes),
            nameof(StorageEnvironmentOptions.EnablePrefetching),
            nameof(StorageEnvironmentOptions.DiscardVirtualMemory),
            nameof(StorageEnvironmentOptions.UseSequentialReadAheadHintForJournalRecovery),
            nameof(StorageEnvironmentOptions.TimeToSyncAfterFlushInSec),
            nameof(StorageEnvironmentOptions.DoNotConsiderMemoryLockFailureAsCatastrophicError),
            nameof(StorageEnvironmentOptions.MaxScratchBufferSize),
            nameof(StorageEnvironmentOptions.PrefetchSegmentSize),
            nameof(StorageEnvironmentOptions.PrefetchResetThreshold),
            nameof(StorageEnvironmentOptions.SyncJournalsCountThreshold),
            nameof(StorageEnvironmentOptions.SyncWritebackBlockSizeInMb),
            nameof(StorageEnvironmentOptions.SyncWritebackMinContiguousSizeInKb),
            nameof(StorageEnvironmentOptions.SyncWritebackBarrierCostThresholdInMs),
            nameof(StorageEnvironmentOptions.SyncWritebackDrainQueueDepthThreshold),
            nameof(StorageEnvironmentOptions.IgnoreInvalidJournalErrors),
            nameof(StorageEnvironmentOptions.SkipChecksumValidationOnDatabaseLoading),
            nameof(StorageEnvironmentOptions.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions),
            nameof(StorageEnvironmentOptions.MaxNumberOfRecyclableJournals),
            nameof(StorageEnvironmentOptions.DisableSparseRegions),
            nameof(StorageEnvironmentOptions.JournalsCompressionAcceleration),
            nameof(StorageEnvironmentOptions.MinimumSharedJournalsMergeCount),
            nameof(StorageEnvironmentOptions.MaxLogFileSize),
        };

        private static readonly Dictionary<string, string> ExemptWithReason = new()
        {
            [nameof(StorageEnvironmentOptions.GenerateNewDatabaseId)] = "per-environment: restore/new-database decision",
            [nameof(StorageEnvironmentOptions.AddToInitLog)] = "per-environment: init-log sink of the owning database",
            [nameof(StorageEnvironmentOptions.SchemaVersion)] = "per-environment: each storage type has its own schema",
            [nameof(StorageEnvironmentOptions.SchemaUpgrader)] = "per-environment: each storage type has its own upgrader",
            [nameof(StorageEnvironmentOptions.OnVersionReadingTransaction)] = "per-environment: index schema detection",
            [nameof(StorageEnvironmentOptions.BeforeSchemaUpgrade)] = "per-environment: system store only",
            [nameof(StorageEnvironmentOptions.AfterDatabaseCreation)] = "per-environment: system store only",
            [nameof(StorageEnvironmentOptions.RootJournal)] = "per-environment: shared-journals wiring",
            [nameof(StorageEnvironmentOptions.OwnsPagers)] = "infrastructure: pager ownership, not a tunable",
            [nameof(StorageEnvironmentOptions.IoMetrics)] = "infrastructure: metrics wiring, not a tunable",
            [nameof(StorageEnvironmentOptions.ManualFlushing)] = "test/tooling hook",
            [nameof(StorageEnvironmentOptions.EnableJournalPoolPrewarming)] = "test hook: disabled by exact-journal-count tests",
            [nameof(StorageEnvironmentOptions.ShouldUseKeyPrefix)] = "tooling: Voron.Recovery only",
            [nameof(StorageEnvironmentOptions.IncrementalBackupEnabled)] = "per-environment: backup feature toggle",
            [nameof(StorageEnvironmentOptions.InitialFileSize)] = "per-environment sizing, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.InitialLogFileSize)] = "per-environment sizing, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.MaxStorageSize)] = "per-environment quota, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.MaxNumberOfPagesInJournalBeforeFlush)] = "derived: overwritten by the ForceUsing32BitsPager setter",
            [nameof(StorageEnvironmentOptions.MaxUnsyncedBytesBeforeSync)] = "Voron default, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.IdleFlushTimeout)] = "Voron default, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.DisposeWaitTime)] = "Voron default, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.LongRunningFlushingWarning)] = "Voron default, not exposed in configuration",
            [nameof(StorageEnvironmentOptions.SupportDurabilityFlags)] = "platform capability, not a tunable",
        };

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Configuration)]
        public void Every_settable_voron_option_is_either_configuration_driven_or_explicitly_exempt()
        {
            var type = typeof(StorageEnvironmentOptions);

            var members = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.SetMethod?.IsPublic == true)
                .Select(p => p.Name)
                .Concat(type.GetFields(BindingFlags.Public | BindingFlags.Instance)
                    .Where(f => f.IsInitOnly == false)
                    .Select(f => f.Name))
                .ToHashSet();

            var unaccounted = members
                .Where(m => ConfigurationDriven.Contains(m) == false && ExemptWithReason.ContainsKey(m) == false)
                .OrderBy(m => m)
                .ToList();

            Assert.True(unaccounted.Count == 0,
                $"New settable member(s) on {nameof(StorageEnvironmentOptions)}: {string.Join(", ", unaccounted)}. " +
                $"Either bind them from configuration in {nameof(VoronOptionsFromConfiguration)}.{nameof(VoronOptionsFromConfiguration.Apply)} " +
                $"and add them to {nameof(ConfigurationDriven)}, or add them to {nameof(ExemptWithReason)} with a reason.");

            var stale = ConfigurationDriven.Concat(ExemptWithReason.Keys)
                .Where(m => members.Contains(m) == false)
                .OrderBy(m => m)
                .ToList();

            Assert.True(stale.Count == 0,
                $"Member(s) listed in this test no longer exist on {nameof(StorageEnvironmentOptions)}: {string.Join(", ", stale)}. " +
                "Remove them here and from the binder if applicable.");

            var overlap = ConfigurationDriven.Intersect(ExemptWithReason.Keys).OrderBy(m => m).ToList();
            Assert.True(overlap.Count == 0, $"Member(s) in both sets: {string.Join(", ", overlap)}");
        }
    }
}
