using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

// ReSharper disable ExplicitCallerInfoArgument

namespace FastTests
{
    public abstract class RavenLowLevelTestBase : TestBase
    {
        private readonly ConcurrentSet<string> _databases = new ConcurrentSet<string>();

        protected RavenLowLevelTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected static void WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtagsForDebug().Values.Min() == etag, timeout));
        }

        protected IDisposable CreatePersistentDocumentDatabase(string dataDirectory, out DocumentDatabase db, [CallerMemberName]string caller = null)
        {
            var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: dataDirectory, caller: caller);
            db = database;
            Debug.Assert(database != null);
            return new DisposableAction(() =>
            {
                DeleteDatabase(database.Name);
            });
        }

        protected DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true, string dataDirectory = null, Action<Dictionary<string, string>> modifyConfiguration = null)
        {
            var name = GetDatabaseName(caller);

            return CreateDatabaseWithName(runInMemory, dataDirectory, modifyConfiguration, name);
        }

        protected DocumentDatabase CreateDatabaseWithName(bool runInMemory, string dataDirectory, Action<Dictionary<string, string>> modifyConfiguration, string name)
        {
            _databases.Add(name);

            if (string.IsNullOrEmpty(dataDirectory))
                dataDirectory = NewDataPath(name);

            var configuration = new Dictionary<string, string>();
            configuration.Add(RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory), int.MaxValue.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), dataDirectory);
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.RunInMemory), runInMemory.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened), "true");

            modifyConfiguration?.Invoke(configuration);

            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrl),
                Database = name
            })
            {
                store.Initialize();

                var doc = new DatabaseRecord(name)
                {
                    Settings = configuration
                };

                var result = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, replicationFactor: 1));

                try
                {
                    return AsyncHelpers.RunSync(() => GetDatabase(name));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Database {result.Name} was created with '{result.RaftCommandIndex}' index.", e);
                }
            }
        }

        protected void DeleteDatabase(string dbName)
        {
            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrl),
                Database = dbName
            })
            {
                store.Initialize();

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
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
                        AsyncHelpers.RunSync(async () =>
                        {
                            try
                            {
                                var (index, _) = await Server.ServerStore.DeleteDatabaseAsync(database, hardDelete: true, fromNodes: new[] { Server.ServerStore.NodeTag }, Guid.NewGuid().ToString());
                                await Server.ServerStore.Cluster.WaitForIndexNotification(index);
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
