using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Server;
using Raven.Server;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3574 : ReplicationBase
    {

        private readonly HashSet<HttpRequestMessage> requestLog = new HashSet<HttpRequestMessage>();
        private bool shouldRecordRequests = false;

        protected override void ModifyServer(RavenDbServer ravenDbServer)
        {
            ravenDbServer.Options.RequestManager.BeforeRequest += RequestManagerOnBeforeRequest;
        }

        private void RequestManagerOnBeforeRequest(object sender, RequestWebApiEventArgs requestWebApiEventArgs)
        {
            if (shouldRecordRequests)
                requestLog.Add(requestWebApiEventArgs.Controller.InnerRequest);
        }

        [Fact]
        public async Task Index_replication_with_index_delete_should_propagate_as_usual()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var testIndex = new RavenDB_3232.TestIndex();
                testIndex.Execute(source);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);

                using (var session = source.OpenSession())
                {
                    session.Store(new RavenDB_3232.Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();
                sourceReplicationTask.IndexReplication.TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.FromSeconds(0);

                SetupReplication(source.DatabaseCommands, destination);

                WaitForIndexing(source);
                sourceReplicationTask.IndexReplication.Execute(); //replicate index create

                source.DatabaseCommands.DeleteIndex(testIndex.IndexName);

                shouldRecordRequests = true;
                sourceReplicationTask.IndexReplication.Execute();


                Assert.Equal(1, requestLog.Count(x => x.Method.Method == "DELETE"));
            }
        }

        [Fact]
        public async Task Index_replication_with_side_by_side_indexes_should_not_propagate_replaced_index_tombstones()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var oldIndexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}"
                };
                var testIndex = new RavenDB_3232.TestIndex();

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                sourceDatabase.StopBackgroundWorkers();

                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                using (var session = source.OpenSession())
                {
                    session.Store(new RavenDB_3232.Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }
                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.IndexReplication.TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.FromSeconds(0);

                sourceReplicationTask.Pause(); //pause replciation task _before_ setting up replication

                SetupReplication(source.DatabaseCommands, destination);

                var mre = new ManualResetEventSlim();

                sourceDatabase.Notifications.OnIndexChange += (database, notification) =>
                {
                    if (notification.Type == IndexChangeTypes.SideBySideReplace)
                        mre.Set();
                };

                shouldRecordRequests = true;
                testIndex.SideBySideExecute(source);

                sourceDatabase.SpinBackgroundWorkers();
                WaitForIndexing(source); //now old index should be a tombstone and side-by-side replaced it.
                mre.Wait();
                sourceReplicationTask.IndexReplication.Execute();

                Assert.Equal(0, requestLog.Count(x => x.Method.Method == "DELETE"));
            }
        }
    }
}
