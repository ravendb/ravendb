using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests
{
    public abstract class RavenLowLevelTestBase : TestBase
    {
        private readonly List<string> _databases = new List<string>();

        protected static void WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtagsForDebug().Values.Min() == etag, timeout));
        }

        private static int _counter;

        protected IDisposable CreatePersistentDocumentDatabase(string dataDirectory, out DocumentDatabase db)
        {
            var database = CreateDocumentDatabase(runInMemory2: false, dataDirectory: dataDirectory);
            db = database;
            return new DisposableAction(() =>
            {
                DeleteDatabase(database.Name);
            });
        }

        protected DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory2 = true, string dataDirectory = null, Action<Dictionary<string, string>> modifyConfiguration = null)
        {
            var name = caller != null ? $"{caller}_{Interlocked.Increment(ref _counter)}" : Guid.NewGuid().ToString("N");

            _databases.Add(name);

            if (string.IsNullOrEmpty(dataDirectory))
                dataDirectory = NewDataPath(name);

            var configuration = new Dictionary<string, string>();
            configuration.Add(RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory), int.MaxValue.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), dataDirectory);
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.RunInMemory), runInMemory2.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened), "true");

            modifyConfiguration?.Invoke(configuration);

            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrls),
                Database = name
            })
            {
                store.Initialize();

                var doc = MultiDatabase.CreateDatabaseDocument(name);
                doc.Settings = configuration;

                store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor: 1));

                return AsyncHelpers.RunSync(() => GetDatabase(name));
            }
        }

        protected void DeleteDatabase(string dbName)
        {
            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrls),
                Database = dbName
            })
            {
                store.Initialize();

                store.Admin.Server.Send(new DeleteDatabaseOperation(dbName, true));
            }
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            if (_databases.Count == 0)
                return;

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                foreach (var database in _databases)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        Server.ServerStore.DatabasesLandlord.UnloadDatabase(database);
                    });

                    exceptionAggregator.Execute(() =>
                    {
                        AsyncHelpers.RunSync(async () =>
                        {
                            try
                            {
                                await Server.ServerStore.DeleteDatabaseAsync(database, hardDelete: true, fromNode: Server.ServerStore.NodeTag);
                            }
                            catch (DatabaseDoesNotExistException)
                            {
                            }
                            catch (Exception e) when (e.InnerException is DatabaseDoesNotExistException)
                            {
                            }
                        });
                    });
                }
            }
        }

        protected static BlittableJsonReaderObject CreateDocument(JsonOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }
    }
}