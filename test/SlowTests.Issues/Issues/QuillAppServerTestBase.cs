using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Commercial.WriteUsageMetering;
using Raven.Server.Config;
using Tests.Infrastructure;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues
{
    public abstract class QuillAppServerTestBase : ClusterTestBase
    {
        private DocumentStore _quillConfig;
        private int _quillConfigReplicationFactor;

        protected QuillAppServerTestBase(ITestOutputHelper output) : base(output)
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.SupervisorSamplePeriod)] = "50";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod)] = "25";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.OnErrorDelayTime)] = "15";

            // the testing flags are set on the server itself, so it must not be shared with other tests
            DoNotReuseServer(DefaultClusterSettings);
        }

        protected sealed class QuillApp
        {
            public string Database { get; set; }
        }

        protected static string AppDocumentId(string database) => Constants.Quill.AppIdPrefix + database;

        /// <summary>
        /// Creates the database like <see cref="RavenTestBase.GetDocumentStore"/> and registers it as a Quill application in the quill-config database.
        /// </summary>
        protected DocumentStore GetQuillAppDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            // create the application first, it bootstraps the cluster the config database is spread over
            var store = GetDocumentStore(options, caller);
            var config = GetOrCreateQuillConfig(options?.Server ?? Server);

            using (var session = config.OpenSession())
            {
                if (_quillConfigReplicationFactor > 1)
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: _quillConfigReplicationFactor - 1);

                session.Store(new QuillApp { Database = store.Database }, AppDocumentId(store.Database));
                session.SaveChanges();
            }

            return store;
        }

        protected IAsyncDocumentSession OpenConfigSession()
        {
            var session = _quillConfig.OpenAsyncSession();
            if (_quillConfigReplicationFactor > 1)
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: _quillConfigReplicationFactor - 1);
            return session;
        }

        protected static WriteUsageApplicationSnapshot SnapshotEntryByDatabase(RavenServer leader, string database)
            => leader.ServerStore.Observer?.LatestWriteUsageSnapshot?.Applications.SingleOrDefault(d => d.ApplicationName == database);

        private DocumentStore GetOrCreateQuillConfig(RavenServer leader)
        {
            if (_quillConfig != null)
                return _quillConfig;

            // reporting is gated on the leader alone, where the observer builds the snapshot
            leader.ServerStore.ForTestingPurposesOnly().ForceWriteUsageReportingEnabled = true;

            _quillConfigReplicationFactor = leader.ServerStore.GetClusterTopology().AllNodes.Count;
            _quillConfig = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => Constants.Quill.ConfigDatabase,
                ReplicationFactor = _quillConfigReplicationFactor,
                Server = leader
            });

            return _quillConfig;
        }
    }
}
