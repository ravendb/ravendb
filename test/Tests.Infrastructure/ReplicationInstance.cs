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
        private ManualResetEventSlim _breakBlockedMre;
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

        public IAsyncDisposable Break()
        {
            _breakBlockedMre = new ManualResetEventSlim(true);
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = _breakBlockedMre;
            return new BreakHandle(this);
        }

        private async Task WaitForResetAsync(ManualResetEventSlim mre, int timeout = 15_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                if (mre?.IsSet == false)
                    return;

                await Task.Delay(16);
            }

            throw new TimeoutException("Replication cycle did not complete within timeout");
        }

        private sealed class BreakHandle : IAsyncDisposable
        {
            private readonly ReplicationInstance _owner;
            public BreakHandle(ReplicationInstance owner) => _owner = owner;

            public async ValueTask DisposeAsync()
            {
                try
                {
                    await _owner.WaitForResetAsync(_owner._breakBlockedMre, timeout: 15_000);
                }
                catch (TimeoutException)
                {
                    // Fail-safe, mirrors MendAsync(): if this instance (e.g. one shard out of many)
                    // genuinely has nothing to replicate, its handler is parked in the heartbeat loop
                    // and never re-checks DebugWaitAndRunReplicationOnce(), so it can't observe a
                    // Reset() here. That's a legitimate "unfinished cycle", not a real failure.
                }
            }
        }

        public async Task MendAsync()
        {
            _database.Configuration.Replication.MaxItemsCount = null;

            // Capture whatever the handler might currently be blocked on - either Break()'s
            // _breakBlockedMre, or the initial MRE from BreakReplicationOnStart - before we
            // replace it, so we can release it below.
            ManualResetEventSlim previousMre = _replicateOnceMre;

            // nextMre starts SET — handler runs its next loop iteration, then calls Reset() and
            // blocks. That Reset() is our signal that the handler is running again.
            ManualResetEventSlim nextMre = new(true);
            _replicateOnceMre = nextMre;
            _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = nextMre;

            _breakBlockedMre?.Set();
            previousMre?.Set();

            try
            {
                // Short timeout: if there's genuinely nothing to replicate, the handler's first
                // iteration after waking returns didWork == false and breaks out before ever
                // re-checking nextMre, so this path legitimately can't observe a Reset() signal.
                await WaitForResetAsync(nextMre, timeout: 3_000);
            }
            catch (TimeoutException)
            {
                // Fail-safe: the handler may have had nothing to replicate and gone straight to
                // WaitForChanges() without re-entering the inner loop to observe nextMre. That's
                // an "unfinished cycle" we can't observe directly, not a real failure.
            }
            finally
            {
                _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
                nextMre.Set();
            }
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

            // Fail-safe: releases the handler if Break() was engaged but MendAsync() was never
            // reached (e.g. an exception escaped the await-using scope), so teardown doesn't hang.
            _breakBlockedMre?.Set();
            _replicateOnceMre?.Set();
        }

        internal static async ValueTask<ReplicationInstance> GetReplicationInstanceAsync(RavenServer server, string databaseName, RavenTestBase.ReplicationManager.ReplicationOptions options)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Make this func private when legacy BreakReplication() is removed");
            return new ReplicationInstance(await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName), databaseName, options);
        }
    }
}
