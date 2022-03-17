using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly IndexesTestBase Indexes;

    public class IndexesTestBase
    {
        private readonly RavenTestBase _parent;

        public IndexesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public IndexErrors[] WaitForIndexingErrors(IDocumentStore store, string[] indexNames = null, TimeSpan? timeout = null, string nodeTag = null, bool? errorsShouldExists = null)
        {
            if (errorsShouldExists is null)
            {
                timeout ??= Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromMinutes(1);
            }
            else
            {
                timeout ??= errorsShouldExists is true
                    ? TimeSpan.FromSeconds(5)
                    : TimeSpan.FromSeconds(1);
            }

            var toWait = new HashSet<string>(indexNames ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                try
                {
                    var indexes = store.Maintenance.Send(new GetIndexErrorsOperation(indexNames, nodeTag));
                    foreach (var index in indexes)
                    {
                        if (index.Errors.Length > 0)
                        {
                            toWait.Remove(index.Name);

                            if (toWait.Count == 0)
                                return indexes;
                        }
                    }
                }
                catch (IndexDoesNotExistException)
                {

                }

                Thread.Sleep(32);
            }

            var msg = $"Got no index error for more than {timeout.Value}.";
            if (toWait.Count != 0)
                msg += $" Still waiting for following indexes: {string.Join(",", toWait)}";

            if (errorsShouldExists is null)
                throw new TimeoutException(msg);

            return null;
        }

        public int WaitForEntriesCount(IDocumentStore store, string indexName, int minEntriesCount, string databaseName = null, TimeSpan? timeout = null, bool throwOnTimeout = true)
        {
            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var entriesCount = -1;

            while (sp.Elapsed < timeout.Value)
            {
                MaintenanceOperationExecutor operations = string.IsNullOrEmpty(databaseName) == false ? store.Maintenance.ForDatabase(databaseName) : store.Maintenance;

                entriesCount = operations.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount;

                if (entriesCount >= minEntriesCount)
                    return entriesCount;

                Thread.Sleep(32);
            }

            if (throwOnTimeout)
                throw new TimeoutException($"It didn't get min entries count {minEntriesCount} for index {indexName}. The index has {entriesCount} entries.");

            return entriesCount;
        }

        public ManualResetEventSlim WaitForIndexBatchCompleted(IDocumentStore store, Func<(string IndexName, bool DidWork), bool> predicate)
        {
            var database = _parent.GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.IndexStore.IndexBatchCompleted += x =>
            {
                if (predicate(x))
                    mre.Set();
            };

            return mre;
        }
    }
}
