using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Utils;
using Xunit;

namespace Tests.Infrastructure
{
    public class ReplicationInstance : IReplicationManager
    {
        private readonly DocumentDatabase _database;
        public readonly string DatabaseName;
        private readonly RavenTestBase.ReplicationManager.ReplicationOptions _options;
        private ManualResetEventSlim _replicateOnceMre;
        private bool _replicateOnceInitialized = false;

        public ReplicationInstance(DocumentDatabase database, string databaseName, RavenTestBase.ReplicationManager.ReplicationOptions options)
        {
            _database = database;
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _options = options;

            if (options.BreakReplicationOnStart)
            {
                _database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
                _replicateOnceMre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            }
        }

        public ReplicationInstance(DocumentDatabase database, string databaseName, bool breakReplication) :
            this(database, databaseName, new RavenTestBase.ReplicationManager.ReplicationOptions { BreakReplicationOnStart = breakReplication })
        {

        }

        public void Break()
        {
            var mre = new ManualResetEventSlim(false);
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre;
        }

        public void Mend()
        {
            var mre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            Assert.NotNull(mre);
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            _database.Configuration.Replication.MaxItemsCount = null;
            mre.Set();
        }

        private void InitializeReplicateOnce()
        {
            _database.Configuration.Replication.MaxItemsCount = _options.MaxItemsCount;

            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
            _replicateOnceMre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;

            _replicateOnceInitialized = true;
        }

        public void ReplicateOnce(string docId)
        {
            if (_replicateOnceInitialized == false)
                InitializeReplicateOnce();

            WaitForReset(); //wait for server to block and wait
            _replicateOnceMre.Set(); //let threads pass
        }

        //wait to reach reset and wait point in server
        private void WaitForReset(int timeout = 15_000)
        {
            var sp = Stopwatch.StartNew();
            while (sp.ElapsedMilliseconds < timeout)
            {
                if (_replicateOnceMre.IsSet == false)
                    return;

                Thread.Sleep(16);
            }

            throw new TimeoutException();
        }

        public virtual async Task EnsureNoReplicationLoopAsync()
        {
            using (var collector = new LiveReplicationPulsesCollector(_database))
            {
                var etag1 = _database.DocumentsStorage.GenerateNextEtag();

                await Task.Delay(3000);

                var etag2 = _database.DocumentsStorage.GenerateNextEtag();

                Assert.True(etag1 + 1 == etag2, $"Replication loop found :( prev: {etag1}, current {etag2}");

                var groups = collector.Pulses.GetAll().GroupBy(p => p.Direction);
                foreach (var group in groups)
                {
                    var key = group.Key;
                    var count = group.Count();
                    Assert.True(count < 50, $"{key} seems to be excessive ({count})");
                }
            }
        }

        public virtual void Dispose()
        {
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            if (_options.KeepMaxItemsCountOnDispose == false)
                _database.Configuration.Replication.MaxItemsCount = null;
            _replicateOnceMre?.Set();
        }

        internal static async ValueTask<ReplicationInstance> GetReplicationInstanceAsync(RavenServer server, string databaseName, RavenTestBase.ReplicationManager.ReplicationOptions options)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Make this func private when legacy BreakReplication() is removed");
            return new ReplicationInstance(await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName), databaseName, options);
        }
    }
}
