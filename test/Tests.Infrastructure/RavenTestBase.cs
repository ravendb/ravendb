using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        private readonly object _getDocumentStoreSync = new object();

        protected virtual DocumentStore GetDocumentStore(
            [CallerMemberName] string caller = null,
            string dbSuffixIdentifier = null,
            string path = null,
            Action<DatabaseRecord> modifyDatabaseRecord = null,
            Func<string, string> modifyName = null,
            string apiKey = null,
            bool ignoreDisabledDatabase = false,
            int replicationFacotr = 1,
            RavenServer defaultServer = null,
            bool waitForDatabasesToBeCreated= false,
            bool deleteDatabaseWhenDisposed = true,
            bool createDatabase = true)
        {
            lock (_getDocumentStoreSync)
            {
                defaultServer = defaultServer??Server;
                var name = caller != null
                    ? $"{caller}_{Interlocked.Increment(ref _counter)}"
                    : Guid.NewGuid().ToString("N");

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
                doc.Settings[RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "10";
                doc.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString();
                doc.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path;
                doc.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)] =
                    "true";
                doc.Settings[
                        RavenConfiguration.GetKey(
                            x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] =
                    int.MaxValue.ToString();
                modifyDatabaseRecord?.Invoke(doc);

                if (createDatabase)
                {
                    TransactionOperationContext context;
                    using (defaultServer.ServerStore.ContextPool.AllocateOperationContext(out context))
                    {
                        context.OpenReadTransaction();
                        if (defaultServer.ServerStore.Cluster.Read(context, Constants.Documents.Prefix + name) != null)
                            throw new InvalidOperationException($"Database '{name}' already exists");
                    }
                }

                var store = new DocumentStore
                {
                    Url = UseFiddler(defaultServer.WebUrls[0]),
                    DefaultDatabase = name,
                    ApiKey = apiKey
                };
                ModifyStore(store);
                store.Initialize();

                if (createDatabase)
                    store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFacotr));
                

                store.AfterDispose += (sender, args) =>
                {
                    if (CreatedStores.TryRemove(store) == false)
                        return; // can happen if we are wrapping the store inside sharded one

                    if (defaultServer.Disposed == false)
                    {
                        var databaseTask = defaultServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name, ignoreDisabledDatabase);
                        if (databaseTask != null && databaseTask.IsCompleted == false)
                            databaseTask.Wait();
                        // if we are disposing store before database had chance to load then we need to wait

                        defaultServer.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                        if(deleteDatabaseWhenDisposed)
                        {
                            var result = store.Admin.Server.Send(new DeleteDatabaseOperation(name, hardDelete));
                            defaultServer.ServerStore.Cluster.WaitForIndexNotification(result.ETag).ConfigureAwait(false).GetAwaiter().GetResult();
                        }
                    }
                };
                CreatedStores.Add(store);
                return store;
            }
        }

        protected virtual void ModifyStore(DocumentStore store)
        {

        }

        public static void WaitForIndexing(IDocumentStore store, string dbName = null, TimeSpan? timeout = null)
        {
            var admin = store.Admin.ForDatabase(dbName);

            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation());
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                if (indexes.All(x => x.IsStale == false && x.Name.StartsWith("ReplacementOf/") == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }
                Thread.Sleep(32);
            }

            var perf = admin.Send(new GetIndexPerformanceStatisticsOperation());
            var errors = admin.Send(new GetIndexErrorsOperation());
            var stats = admin.Send(new GetIndexesStatisticsOperation());

            var total = new
            {
                Errors = errors,
                Stats = stats,
                Performance = perf
            };

            var file = Path.GetTempFileName() + ".json";
            using (var stream = File.Open(file, FileMode.OpenOrCreate))
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(total);
                var json = context.ReadObject(djv, "errors");
                writer.WriteObject(json);
                writer.Flush();
            }

            var statistics = admin.Send(new GetStatisticsOperation());

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
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(500);
            } while (documentStore.Commands().Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        /// <summary>
        /// Get command for new client tests
        /// </summary>
        /// <param name="session"></param>
        /// <param name="ids"></param>
        /// <param name="etag"></param>
        /// <param name="documentInfo"></param>
        /// <returns></returns>
        protected static BlittableJsonReaderObject GetCommand(IDocumentSession session, string[] ids,
            out DocumentInfo documentInfo)
        {
            var command = new GetDocumentCommand
            {
                Ids = ids,
                Context = session.Advanced.Context
            };
            if (session.Advanced.RequestExecutor == null)
                Console.WriteLine();
            session.Advanced.RequestExecutor.Execute(command, session.Advanced.Context);
            var document = (BlittableJsonReaderObject)command.Result.Results[0];
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");
            string id;
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out id) == false)
                throw new InvalidOperationException("Document must have an id");
            long? etag;
            if (metadata.TryGet(Constants.Documents.Metadata.Etag, out etag) == false)
                throw new InvalidOperationException("Document must have an etag");
            documentInfo = new DocumentInfo
            {
                Id = id,
                Document = document,
                Metadata = metadata,
                ETag = etag
            };
            return document;
        }

        protected void PutCommand(IDocumentSession session, object entity, string id, bool skipMetadata = false)
        {
            var documentInfo = new DocumentInfo
            {
                Entity = entity,
                Id = id
            };

            if (skipMetadata == false)
            {
                var tag = session.Advanced.DocumentStore.Conventions.GetCollectionName(entity);

                var metadata = new DynamicJsonValue();
                if (tag != null)
                    metadata[Constants.Documents.Metadata.Collection] = tag;

                documentInfo.Metadata = session.Advanced.Context.ReadObject(metadata, id);
            }

            documentInfo.Document = session.Advanced.EntityToBlittable.ConvertEntityToBlittable(documentInfo.Entity, documentInfo);

            var putCommand = new PutDocumentCommand
            {
                Id = id,
                Etag = documentInfo.ETag,
                Document = documentInfo.Document,
                Context = session.Advanced.Context
            };
            session.Advanced.RequestExecutor.Execute(putCommand, session.Advanced.Context);
        }

        protected Task PutCommandAsync(IAsyncDocumentSession session, object entity, string id, 
            bool skipMetadata = false, CancellationToken token = default(CancellationToken))
        {
            var documentInfo = new DocumentInfo
            {
                Entity = entity,
                Id = id
            };

            if (skipMetadata == false)
            {
                var tag = session.Advanced.DocumentStore.Conventions.GetCollectionName(entity);

                var metadata = new DynamicJsonValue();
                if (tag != null)
                    metadata[Constants.Documents.Metadata.Collection] = tag;

                documentInfo.Metadata = session.Advanced.Context.ReadObject(metadata, id);
            }

            documentInfo.Document = session.Advanced.EntityToBlittable.ConvertEntityToBlittable(documentInfo.Entity, documentInfo);

            var putCommand = new PutDocumentCommand
            {
                Id = id,
                Etag = documentInfo.ETag,
                Document = documentInfo.Document,
                Context = session.Advanced.Context
            };
            return session.Advanced.RequestExecutor.ExecuteAsync(putCommand, session.Advanced.Context, token);
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);
            CreatedStores.Clear();
        }
    }
}