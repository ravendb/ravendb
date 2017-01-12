using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Data.Indexes;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;

namespace FastTests
{
    public class RavenTestBase : TestBase
    {
        private static int _counter;

        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(DocumentStore store)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
        }

        protected virtual DocumentStore GetDocumentStore(
            [CallerMemberName] string caller = null,
            string dbSuffixIdentifier = null,
            string path = null,
            Action<DatabaseDocument> modifyDatabaseDocument = null,
            Func<string, string> modifyName = null,
            string apiKey = null)
        {
            var name = caller != null ? $"{caller}_{Interlocked.Increment(ref _counter)}" : Guid.NewGuid().ToString("N");

            if (dbSuffixIdentifier != null)
                name = $"{name}_{dbSuffixIdentifier}";

            if (modifyName != null)
                name = modifyName(name);

            var hardDelete = true;
            var runInMemory = true;

            if (path == null)
                path = NewDataPath(name);
            else
            {
                hardDelete = false;
                runInMemory = false;
            }

            var doc = MultiDatabase.CreateDatabaseDocument(name);
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString();
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path;
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)] = "true";
            doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString();
            modifyDatabaseDocument?.Invoke(doc);

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                if (Server.ServerStore.Read(context, Constants.Database.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }

            var store = new DocumentStore
            {
                Url = UseFiddler(Server.WebUrls[0]),
                DefaultDatabase = name,
                ApiKey = apiKey
            };
            ModifyStore(store);
            store.Initialize();

            store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);
            store.AfterDispose += (sender, args) =>
            {
                if (CreatedStores.TryRemove(store) == false)
                    return; // can happen if we are wrapping the store inside sharded one

                if (Server.Disposed == false)
                {
                    var databaseTask = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);
                    if (databaseTask != null && databaseTask.IsCompleted == false)
                        databaseTask.Wait(); // if we are disposing store before database had chance to load then we need to wait

                    store.DatabaseCommands.GlobalAdmin.DeleteDatabase(name, hardDelete: hardDelete);
                }
            };
            CreatedStores.Add(store);
            return store;
        }

        protected virtual void ModifyStore(DocumentStore store)
        {
        }

        public static void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var databaseCommands = store.DatabaseCommands;
            if (database != null)
            {
                databaseCommands = databaseCommands.ForDatabase(database);
            }

            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));


            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = databaseCommands.GetStatistics();
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                if (indexes.All(x => x.IsStale == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }
                Thread.Sleep(32);
            }

            var request = databaseCommands.CreateRequest("/indexes/performance", HttpMethod.Get);
            var perf = request.ReadResponseJson();
            request = databaseCommands.CreateRequest("/indexes/errors", HttpMethod.Get);
            var errors = request.ReadResponseJson();

            request = databaseCommands.CreateRequest("/indexes/stats", HttpMethod.Get);
            var stats = request.ReadResponseJson();

            var total = new RavenJObject
            {
                ["Errors"] = errors,
                ["Stats"] = stats,
                ["Performance"] = perf
            };

            var file = Path.GetTempFileName() + ".json";
            using (var writer = File.CreateText(file))
            {
                var jsonTextWriter = new JsonTextWriter(writer);
                total.WriteTo(jsonTextWriter);
                jsonTextWriter.Flush();
            }

            var statistics = databaseCommands.GetStatistics();

            var corrupted = statistics.Indexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + file);
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + file);
        }

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            string url = documentStore.Url;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase);
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded +
                                "&withStop=true";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);

            CreatedStores.Clear();
        }
    }
}