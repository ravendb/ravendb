// -----------------------------------------------------------------------
//  <copyright file="Batches.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
    public class Batches : RaftTestBase
    {
        [Theory]
        [PropertyData("Nodes")]
        public void BatchCommandsShouldWork(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            var store1 = clusterStores[0];

            store1.DatabaseCommands.Batch(new List<ICommandData>
                                          {
                                              new PutCommandData
                                              {
                                                  Key = "keys/1",
                                                  Etag = null,
                                                  Document = new RavenJObject(),
                                                  Metadata = new RavenJObject()
                                              },
                                              new PutCommandData
                                              {
                                                  Key = "keys/2",
                                                  Etag = null,
                                                  Document = new RavenJObject(),
                                                  Metadata = new RavenJObject()
                                              },
                                          });

            using (ForceNonClusterRequests(clusterStores))
            {
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/1"));
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/2"));
            }

               

            store1.DatabaseCommands.Batch(new List<ICommandData>
                                          {
                                              new DeleteCommandData
                                              {
                                                  Key = "keys/2",
                                                  Etag = null
                                              }
                                          });

            using (ForceNonClusterRequests(clusterStores))
            {
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/1"));
                clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands, "keys/2"));
            }
        }
    }
}
