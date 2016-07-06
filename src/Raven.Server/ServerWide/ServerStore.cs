using System;
using System.Collections.Generic;
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

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public unsafe class ServerStore : IDisposable
    {
        private CancellationTokenSource shutdownNotification;

        public CancellationToken ServerShutdown => shutdownNotification.Token;

        private static readonly ILog Log = LogManager.GetLogger(typeof(ServerStore));

        private StorageEnvironment _env;

        private UnmanagedBuffersPool _pool;

        public readonly DatabasesLandlord DatabasesLandlord;

        private readonly IList<IDisposable> toDispose = new List<IDisposable>();
        public readonly RavenConfiguration Configuration;
        private readonly LoggerSetup _loggerSetup;
        public readonly MetricsScheduler MetricsScheduler;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);

        public ServerStore(RavenConfiguration configuration, LoggerSetup loggerSetup)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            MetricsScheduler = new MetricsScheduler();
            Configuration = configuration;
            _loggerSetup = loggerSetup;

            DatabasesLandlord = new DatabasesLandlord(this, _loggerSetup);
        }

        public TransactionContextPool ContextPool;

        private Timer _timer;

        public void Initialize()
        {
            shutdownNotification = new CancellationTokenSource();

            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Starting to open server store for {0}", Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory);
            }
            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Configuration.Core.DataDirectory);

            options.SchemaVersion = 1;

            try
            {
                _env = new StorageEnvironment(options, _loggerSetup);
                using (var tx = _env.WriteTransaction())
                {
                    tx.CreateTree("items");
                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.FatalException(
                        "Could not open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory), e);
                }
                options.Dispose();
                throw;
            }

            _pool = new UnmanagedBuffersPool("ServerStore");// 128MB should be more than big enough for the server store
            ContextPool = new TransactionContextPool(_pool, _env);
            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext ctx, string id)
        {
            var dbs = ctx.Transaction.InnerTransaction.ReadTree("items");
            var result = dbs.Read(id);
            if (result == null)
                return null;
            return new BlittableJsonReaderObject(result.Reader.Base, result.Reader.Length, ctx);
        }

        public void Delete(TransactionOperationContext ctx, string id)
        {
            var dbs = ctx.Transaction.InnerTransaction.ReadTree("items");
            dbs.Delete(id);
        }

        public class Item
        {
            public string Key;
            public BlittableJsonReaderObject Data;
        }

        public IEnumerable<Item> StartingWith(TransactionOperationContext ctx, string prefix, int start, int take)
        {
            var dbs = ctx.Transaction.InnerTransaction.ReadTree("items");
            using (var it = dbs.Iterate(true))
            {
                it.RequiredPrefix = Slice.From(ctx.Allocator, prefix, ByteStringType.Immutable);
                if (it.Seek(it.RequiredPrefix) == false)
                    yield break;

                do
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItem(ctx, it);
                } while (it.MoveNext());
            }
        }

        private static Item GetCurrentItem(JsonOperationContext ctx, IIterator it)
        {
            var readerForCurrent = it.CreateReaderForCurrent();
            return new Item
            {
                Data = new BlittableJsonReaderObject(readerForCurrent.Base, readerForCurrent.Length, ctx),
                Key = it.CurrentKey.ToString()
            };
        }


        public void Write(TransactionOperationContext ctx, string id, BlittableJsonReaderObject doc)
        {
            var dbs = ctx.Transaction.InnerTransaction.ReadTree("items");

            var ptr = dbs.DirectAdd(id, doc.Size);
            doc.CopyTo(ptr);
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
                        Log.WarnException("Error during idle operation run for " + db.Key, e);
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
                    Log.WarnException("Error during idle operations for the server", e);
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