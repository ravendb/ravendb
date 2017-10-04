// -----------------------------------------------------------------------
//  <copyright file="RavenDB-6755.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Replication.Tasks;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6755 : ReplicationBase
    {
        [Fact]
        public async Task skip_side_by_side_index_replication()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                using (var session = source.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                        session.Store(new RavenDB_3573.User
                        {
                            Name = "User - " + i
                        });

                    session.SaveChanges();
                }

                WaitForIndexing(source);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                SetupReplicationWithSkipIndexReplication(source.DatabaseCommands, destination);

                var testIndex = new RavenDB_3573.UserIndex();

                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };

                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);
                testIndex.SideBySideExecute(source);

                var sourceIndexes = source.DatabaseCommands.GetIndexes(0, int.MaxValue);
                Assert.True(sourceIndexes.Any(x => x.Name == "UserIndex"));
                Assert.True(sourceIndexes.Any(x => x.Name == Constants.SideBySideIndexNamePrefix + "UserIndex"));

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.IndexReplication.Execute();

                SpinWait.SpinUntil(() =>
                {
                    var destinationIndexes = destination.DatabaseCommands.GetIndexes(0, int.MaxValue);
                    Assert.False(destinationIndexes.Any(x => x.Name == "UserIndex"));
                    Assert.False(destinationIndexes.Any(x => x.Name == Constants.SideBySideIndexNamePrefix + "UserIndex"));
                    return false;
                }, 5000);
            }
        }
    }
}
