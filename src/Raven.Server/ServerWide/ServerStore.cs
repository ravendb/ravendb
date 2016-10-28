using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Voron;
using Voron.Data;
using Sparrow;
using Sparrow.Logging;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public unsafe class ServerStore : IDisposable
    {
        private CancellationTokenSource _shutdownNotification;

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private static Logger _logger;

        private StorageEnvironment _env;

        private readonly TableSchema _itemsSchema;

        public readonly DatabasesLandlord DatabasesLandlord;

        private readonly IList<IDisposable> toDispose = new List<IDisposable>();
        public readonly RavenConfiguration Configuration;
        public readonly IoMetrics IoMetrics;
        public readonly AlertsStorage Alerts;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);

        public ServerStore(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            IoMetrics = new IoMetrics(8,8); // TODO:: increase this to 256,256 ?
            Configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<ServerStore>("ServerStore");
            DatabasesLandlord = new DatabasesLandlord(this);

            Alerts = new AlertsStorage("Raven/Server");

            // We use the follow format for the items data
            // { lowered key, key, data }
            _itemsSchema = new TableSchema();
            _itemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 0
            });
        }

        public TransactionContextPool ContextPool;

        private Timer _timer;

        public void Initialize()
        {
            _shutdownNotification = new CancellationTokenSource();

            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory));

            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(Configuration.Core.DataDirectory)
                : StorageEnvironmentOptions.ForPath(Configuration.Core.DataDirectory);

            options.SchemaVersion = 1;

            try
            {
                StorageEnvironment.MaxConcurrentFlushes = Configuration.Storage.MaxConcurrentFlushes;
                _env = new StorageEnvironment(options);
                using (var tx = _env.WriteTransaction())
                {
                    tx.DeleteTree("items");// note the different casing, we remove the old items tree 
                    _itemsSchema.Create(tx, "Items");
                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(
                        "Could not open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory), e);
                options.Dispose();
                throw;
            }

            ContextPool = new TransactionContextPool(_env);
            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            Alerts.Initialize(_env, ContextPool);
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext ctx, string id)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

            TableValueReader reader;
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                reader = items.ReadByKey(key);
            }
            if (reader == null)
                return null;
            int size;
            var ptr = reader.Read(2, out size);
            return new BlittableJsonReaderObject(ptr, size, ctx);
        }

        public void Delete(TransactionOperationContext ctx, string id)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                items.DeleteByKey(key);
            }
        }

        public class Item
        {
            public string Key;
            public BlittableJsonReaderObject Data;
        }

        public IEnumerable<Item> StartingWith(TransactionOperationContext ctx, string prefix, int start, int take)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");
            Slice loweredPrefix;
            using (Slice.From(ctx.Allocator, prefix.ToLowerInvariant(), out loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKey(loweredPrefix, startsWith: true))
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                        yield break;
                    yield return GetCurrentItem(ctx, result);
                }
            }
        }

        private static Item GetCurrentItem(JsonOperationContext ctx, TableValueReader reader)
        {
            int size;
            return new Item
            {
                Data = new BlittableJsonReaderObject(reader.Read(2, out size), size, ctx),
                Key = Encoding.UTF8.GetString(reader.Read(1, out size), size)
            };
        }


        public void Write(TransactionOperationContext ctx, string id, BlittableJsonReaderObject doc)
        {
            Slice idAsSlice;
            Slice loweredId;
            using (Slice.From(ctx.Allocator, id, out idAsSlice))
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out loweredId))
            {
                var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

                items.Set(new TableValueBuilder
                {
                    loweredId,
                    idAsSlice,
                    {doc.BasePointer, doc.Size}
                });
            }
        }

        public void Dispose()
        {

            _shutdownNotification.Cancel();

            ContextPool?.Dispose();

            toDispose.Add(_env);
            toDispose.Add(DatabasesLandlord);

            var errors = new List<Exception>();
            foreach (var disposable in toDispose)
            {
                try
                {
                    disposable?.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }
            if (errors.Count != 0)
                throw new AggregateException(errors);
        }

        private void IdleOperations(object state)
        {
            try
            {
                foreach (var db in DatabasesLandlord.ResourcesStoresCache)
                {
                    try
                    {
                        if (db.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = db.Value.Result;

                        if (DatabaseNeedToRunIdleOperations(database))
                            database.RunIdleOperations();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    //var databasesToCleanup = DatabasesLandlord.LastRecentlyUsed
                    //   .Where(x => SystemTime.UtcNow - x.Value > maxTimeDatabaseCanBeIdle)
                    //   .Select(x => x.Key)
                    //   .ToArray();

                    //foreach (var databaseToCleanup in databasesToCleanup)
                    //{
                    //    // intentionally inside the loop, so we get better concurrency overall
                    //    // since shutting down a database can take a while
                    //    DatabasesLandlord.Cleanup(databaseToCleanup, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle, shouldSkip: database => database.Configuration.RunInMemory);
                    //}

                    // TODO [ppekrol]
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error during idle operations for the server", e);
                }
            }
            finally
            {
                try
                {
                    _timer.Change(_frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private static bool DatabaseNeedToRunIdleOperations(DocumentDatabase database)
        {
            //var dateTime = SystemTime.UtcNow;
            //if ((dateTime - database.WorkContext.LastWorkTime).TotalMinutes > 5)
            //    return true;
            //if ((dateTime - database.WorkContext.LastIdleTime).TotalHours > 2)
            //    return true;
            //return false;
            // TODO [ppekrol]

            return true;
        }
    }
}