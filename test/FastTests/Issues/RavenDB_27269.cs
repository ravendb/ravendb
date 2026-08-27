using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents.Indexes.Configuration;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27269(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Indexes)]
        public void PerIndexSettingsMustDeclareHowTheIndexIsUpdated()
        {
            // A setting that can be set per index ends up in the index definition's Configuration, so changing it
            // reaches SingleIndexConfiguration.CalculateUpdateType and decides what IndexStore.GetIndexCreationOptions
            // does with the index. IndexUpdateType.None means "nothing to decide": CalculateUpdateType returns None,
            // the switch there falls through every remaining difference check and the method ends on its catch-all
            // `return IndexCreationOptions.Update` - rebuilding the index side by side for a setting that declared it
            // needs no index-side work at all. So per-index settings must pick one of the two real answers:
            //   Refresh - the value is read on the fly (next indexing batch, next query, background work),
            //             so swapping the configuration on the live index is enough,
            //   Reset   - the value is baked into what the index stores (see IndexStorage.PersistConfiguration),
            //             so only a newly built index can pick it up.
            var offenders = new List<string>();

            foreach (var property in typeof(SingleIndexConfiguration).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var entry = property.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).FirstOrDefault();
                if (entry == null || entry.Scope != ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)
                    continue;

                var updateType = property.GetCustomAttribute<IndexUpdateTypeAttribute>();

                // CalculateUpdateType dereferences this attribute without a null check, so a missing one is a crash.
                if (updateType == null)
                {
                    offenders.Add($"{entry.Key} - no {nameof(IndexUpdateTypeAttribute)}");
                    continue;
                }

                if (updateType.UpdateType == IndexUpdateType.None)
                    offenders.Add($"{entry.Key} - {nameof(IndexUpdateType)}.{nameof(IndexUpdateType.None)}");
            }

            Assert.True(offenders.Count == 0,
                $"Per-index settings must be marked {nameof(IndexUpdateType)}.{nameof(IndexUpdateType.Refresh)} or " +
                $"{nameof(IndexUpdateType)}.{nameof(IndexUpdateType.Reset)}, otherwise changing them rebuilds the index " +
                $"side by side for nothing:{Environment.NewLine}{string.Join(Environment.NewLine, offenders)}");
        }
    }
}
