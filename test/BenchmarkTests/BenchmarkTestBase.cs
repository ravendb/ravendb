using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;

namespace BenchmarkTests
{
    public abstract class BenchmarkTestBase : RavenTestBase
    {
        public abstract Task InitAsync(DocumentStore store);

        protected override RavenServer GetNewServer(IDictionary<string, string> customSettings = null, bool deletePrevious = true, bool runInMemory = true, string partialPath = null,
            string customConfigPath = null)
        {
            if (customSettings == null)
                customSettings = new Dictionary<string, string>();

            customSettings[RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = int.MaxValue.ToString();

            return base.GetNewServer(customSettings, deletePrevious, false, partialPath, customConfigPath);
        }

        protected DocumentStore GetSimpleDocumentStore(string databaseName, bool deleteDatabaseOnDispose = true)
        {
            var store = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = databaseName
            };

            if (deleteDatabaseOnDispose)
            {
                store.BeforeDispose += (sender, args) =>
                {
                    try
                    {
                        store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true));
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                };
            }

            store.Initialize();

            return store;
        }

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName]string caller = null)
        {
            if (options == null)
                options = new Options();

            options.ModifyDatabaseRecord = record => record.Settings.Remove(RavenConfiguration.GetKey(x => x.Core.RunInMemory));

            return base.GetDocumentStore(options, caller);
        }

        protected async Task WaitForIndexAsync(DocumentStore store, string databaseName, string indexName, TimeSpan? timeout = null)
        {
            if (timeout == null)
                timeout = TimeSpan.FromMinutes(10);

            var admin = store.Maintenance.ForDatabase(databaseName);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var indexStats = await admin.SendAsync(new GetIndexStatisticsOperation(indexName));
                if (indexStats == null)
                    IndexDoesNotExistException.ThrowFor(indexName);

                if (indexStats.Status != IndexRunningStatus.Running)
                    throw new InvalidOperationException($"Index '{indexName}' is not running!");

                if (indexStats.State != IndexState.Idle && indexStats.State != IndexState.Normal)
                    throw new InvalidOperationException($"Index '{indexName}' state ({indexStats.State}) is invalid!");

                if (indexStats.IsStale == false)
                    return;

                await Task.Delay(32);
            }

            throw new TimeoutException($"The index '{indexName}' stayed stale for more than {timeout.Value}.");
        }
    }
}
