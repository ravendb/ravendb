using System;
using System.Collections.Generic;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public class SorterCompilationCache : AbstractCompilationCache<SorterFactory>
    {
        public static readonly SorterCompilationCache Instance = new();

        private SorterCompilationCache()
        {
        }


        protected override bool DatabaseRecordContainsItem(RawDatabaseRecord databaseRecord, string name)
        {
            return databaseRecord.Sorters != null && databaseRecord.Sorters.ContainsKey(name);
        }

        protected override IEnumerable<(string Name, string Code)> GetItemsFromDatabaseRecord(RawDatabaseRecord databaseRecord)
        {
            if (databaseRecord.Sorters == null || databaseRecord.Sorters.Count == 0)
                yield break;

            foreach (var kvp in databaseRecord.Sorters)
                yield return (kvp.Value.Name, kvp.Value.Code);
        }

        protected override IEnumerable<(string Name, string Code)> GetItemsFromCluster(ServerStore serverStore, TransactionOperationContext context)
        {
            foreach (var kvp in serverStore.Cluster.ItemsStartingWith(context, PutServerWideSorterCommand.Prefix, 0, long.MaxValue))
            {
                var sorterDefinition = JsonDeserializationServer.SorterDefinition(kvp.Value);
                sorterDefinition.Validate();

                yield return (sorterDefinition.Name, sorterDefinition.Code);
            }
        }

        protected override SorterFactory CompileItem(string name, string code)
        {
            try
            {
                return SorterCompiler.Compile(name, code);
            }
            catch (Exception e)
            {
                return new FaultySorterFactory(name, e);
            }
        }
    }
}
