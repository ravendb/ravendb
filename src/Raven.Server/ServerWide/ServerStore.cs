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
        private CancellationTokenSource shutdownNotification;

        public CancellationToken ServerShutdown => shutdownNotification.Token;

        private static Logger _logger;

        private StorageEnvironment _env;

        private UnmanagedBuffersPool _pool;
        private TableSchema _itemsSchema;

        public readonly DatabasesLandlord DatabasesLandlord;

        private readonly IList<IDisposable> toDispose = new List<IDisposable>();
        public readonly RavenConfiguration Configuration;
        private readonly LoggerSetup _loggerSetup;
        public readonly MetricsScheduler MetricsScheduler;
        public readonly IoMetrics IoMetrics;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);

        public ServerStore(RavenConfiguration configuration, LoggerSetup loggerSetup)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            MetricsScheduler = new MetricsScheduler(loggerSetup);
            IoMetrics = new IoMetrics(8,8); // TODO:: increase this to 256,256 ?
            Configuration = configuration;
            _loggerSetup = loggerSetup;
            _logger = _loggerSetup.GetLogger<ServerStore>("ServerStore");
            DatabasesLandlord = new DatabasesLandlord(this, _loggerSetup);

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
            shutdownNotification = new CancellationTokenSource();

            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory));

            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Configuration.Core.DataDirectory);

            options.SchemaVersion = 1;

            try
            {
                _env = new StorageEnvironment(options, _loggerSetup);
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

            _pool = new UnmanagedBuffersPool("ServerStore");// 128MB should be more than big enough for the server store
            ContextPool = new TransactionContextPool(_pool, _env);
            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext ctx, string id)
        {
            var items = new Table(_itemsSchema, "Items", ctx.Transaction.InnerTransaction);
            var reader = items.ReadByKey(Slice.From(ctx.Allocator, id.ToLowerInvariant()));
            if (reader == null)
                return null;
            int size;
            var ptr = reader.Read(2, out size);
            return new BlittableJsonReaderObject(ptr, size, ctx);
        }

        public void Delete(TransactionOperationContext ctx, string id)
        {
            var items = new Table(_itemsSchema, "Items", ctx.Transaction.InnerTransaction);
            items.DeleteByKey(Slice.From(ctx.Allocator, id.ToLowerInvariant()));
        }

        public class Item
        {
            public string Key;
            public BlittableJsonReaderObject Data;
        }

        public IEnumerable<Item> StartingWith(TransactionOperationContext ctx, string prefix, int start, int take)
        {
            var items = new Table(_itemsSchema, "Items", ctx.Transaction.InnerTransaction);
            var loweredPrefix = Slice.From(ctx.Allocator, prefix.ToLowerInvariant());
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
            var idAsSlice = Slice.From(ctx.Allocator, id);
            var loweredId = Slice.From(ctx.Allocator, id.ToLowerInvariant());
            var items = new Table(_itemsSchema, "Items", ctx.Transaction.InnerTransaction);


            items.Set(new TableValueBuilder
            {
                loweredId,
                idAsSlice,
                {doc.BasePointer, doc.Size}
            });
        }

        public void Dispose()
        {

            shutdownNotification.Cancel();

            ContextPool?.Dispose();

            toDispose.Add(_pool);
            toDispose.Add(_env);
            toDispose.Add(DatabasesLandlord);
            toDispose.Add(MetricsScheduler);

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