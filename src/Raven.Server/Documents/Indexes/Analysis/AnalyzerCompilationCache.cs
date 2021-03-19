using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Analysis
{
    public class AnalyzerCompilationCache : AbstractCompilationCache<Type>
    {
        public static readonly AnalyzerCompilationCache Instance = new();

        private AnalyzerCompilationCache()
        {
        }

        protected override bool DatabaseRecordContainsItem(DatabaseRecord databaseRecord, string name)
        {
            return databaseRecord.Analyzers != null && databaseRecord.Analyzers.ContainsKey(name);
        }

        protected override IEnumerable<(string Name, string Code)> GetItemsFromDatabaseRecord(DatabaseRecord databaseRecord)
        {
            if (databaseRecord.Analyzers == null || databaseRecord.Analyzers.Count == 0)
                yield break;

            foreach (var kvp in databaseRecord.Analyzers)
                yield return (kvp.Value.Name, kvp.Value.Code);
        }

        protected override IEnumerable<(string Name, string Code)> GetItemsFromCluster(ServerStore serverStore, TransactionOperationContext context)
        {
            foreach (var kvp in serverStore.Cluster.ItemsStartingWith(context, PutServerWideAnalyzerCommand.Prefix, 0, long.MaxValue))
            {
                var analyzerDefinition = JsonDeserializationServer.AnalyzerDefinition(kvp.Value);
                analyzerDefinition.Validate();

                yield return (analyzerDefinition.Name, analyzerDefinition.Code);
            }
        }

        protected override Type CompileItem(string name, string code)
        {
            return AnalyzerCompiler.Compile(name, code);
        }
    }
}
