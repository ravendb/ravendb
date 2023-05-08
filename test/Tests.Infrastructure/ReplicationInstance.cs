using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        private ManualResetEventSlim _replicateOnceMre;
        private bool _replicateOnceInitialized = false;

        protected ReplicationInstance(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public ReplicationInstance(DocumentDatabase database, string databaseName, bool breakReplication)
        {
            _database = database;
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

            if (breakReplication)
            {
                _database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
                _replicateOnceMre = _database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            }
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
            _database.Configuration.Replication.MaxItemsCount = 1;

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

                Assert.True(etag1 + 1 == etag2, "Replication loop found :(");

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
            if (_replicateOnceInitialized)
            {
                WaitForReset();
                _replicateOnceMre.Set();
            }

            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
            _database.Configuration.Replication.MaxItemsCount = null;
        }

        internal static async ValueTask<ReplicationInstance> GetReplicationInstanceAsync(RavenServer server, string databaseName, bool breakReplication = false)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Make this func private when legacy BreakReplication() is removed");
            return new ReplicationInstance(await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName), databaseName, breakReplication);
        }
    }
}
