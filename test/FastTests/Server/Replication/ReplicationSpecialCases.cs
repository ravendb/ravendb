using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Json;
using Raven.Server.Extensions;
using Sparrow.Json;
using Xunit;
using Raven.NewClient.Abstractions.Extensions;

namespace FastTests.Server.Replication
{
    public class ReplicationSpecialCases : ReplicationTestsBase
    {
        [Fact]
        public void TopologyNode_with_circular_reference_should_work_with_blittable()
        {
            var topologyNode = new TopologyNode
            {
                Node = new ServerNode { DbId = "A" },
                Outgoing = new List<TopologyNode>
                {
                    new TopologyNode
                    {
                        Node = new ServerNode {DbId = "B"},
                        Outgoing = new List<TopologyNode>()
                    }
                }
            };

            var outgoingLevel2 = topologyNode.Outgoing.First().Outgoing;
            outgoingLevel2.Add(topologyNode);

            var dynamicJson = topologyNode.ToJson();
            using (var context = new JsonOperationContext(1024, 1024))
            using (var blittableJson = context.ReadObject(dynamicJson, string.Empty))
            {
                var deserializedTopologyMasterNode = JsonManualDeserialization.ConvertToTopologyNode(blittableJson);
                Assert.Equal("A", deserializedTopologyMasterNode.Node.DbId);

                Assert.Equal(1, deserializedTopologyMasterNode.Outgoing.Count);
                Assert.Equal("B", deserializedTopologyMasterNode.Outgoing.First().Node.DbId);

                Assert.Equal(1, deserializedTopologyMasterNode.Outgoing.First().Outgoing.Count);
                Assert.Equal("A", deserializedTopologyMasterNode.Outgoing.First().Outgoing.First().Node.DbId);
            }
        }

        [Fact]
        public void TopologyNode_with_multilevel_tree_should_work_with_blittable()
        {
            var topologyMasterNode = new TopologyNode
            {
                Node = new ServerNode
                {
                    DbId = Guid.NewGuid().ToString(),
                    Url = "the_url",
                    ApiKey = "ABC",
                    Database = "the_database"
                },
                SpecifiedCollections = new Dictionary<string, string>
                {
                    {"AA","BB"},
                    {"CC","DD"}
                },
                Outgoing = new List<TopologyNode>
                {
                     new TopologyNode
                            {
                                Node = new ServerNode
                                {
                                    DbId = Guid.NewGuid().ToString(),
                                    Url = "the_url31",
                                    ApiKey = "ABC31",
                                    Database = "the_database31"
                                },
                                SpecifiedCollections = new Dictionary<string, string>
                                {
                                    {"ZZ2","LL2"}
                                }
                            },
                    new TopologyNode
                    {
                        Node = new ServerNode
                        {
                            DbId = Guid.NewGuid().ToString(),
                            Url = "the_url2",
                            ApiKey = "ABC2",
                            Database = "the_database2"
                        },
                        SpecifiedCollections = new Dictionary<string, string>
                        {
                            {"DD","EE"},
                            {"FF","GG"}
                        },
                        Outgoing = new List<TopologyNode>
                        {
                            new TopologyNode
                            {
                                Node = new ServerNode
                                {
                                    DbId = Guid.NewGuid().ToString(),
                                    Url = "the_url3",
                                    ApiKey = "ABC3",
                                    Database = "the_database3"
                                },
                                SpecifiedCollections = new Dictionary<string, string>
                                {
                                    {"ZZ","LL"}
                                }
                            }
                        }
                    }
                }
            };

            var dynamicJson = topologyMasterNode.ToJson();
            using (var context = new JsonOperationContext(1024, 1024))
            using (var blittableJson = context.ReadObject(dynamicJson,string.Empty))
            {
                var deserializedTopologyMasterNode = JsonManualDeserialization.ConvertToTopologyNode(blittableJson);
                AssertEqual(topologyMasterNode, deserializedTopologyMasterNode);
                Assert.Equal(2, deserializedTopologyMasterNode.Outgoing.Count);

                var internalTopologyNode = topologyMasterNode.Outgoing.First();
                var internalDeserializedTopologyNode = deserializedTopologyMasterNode.Outgoing.First();
                AssertEqual(internalTopologyNode, internalDeserializedTopologyNode);
                Assert.Empty(internalDeserializedTopologyNode.Outgoing);

                var internalTopologyNode2 = topologyMasterNode.Outgoing.Skip(1).First();
                var internalDeserializedTopologyNode2 = deserializedTopologyMasterNode.Outgoing.Skip(1).First();
                AssertEqual(internalTopologyNode2, internalDeserializedTopologyNode2);
                Assert.Equal(1, internalDeserializedTopologyNode2.Outgoing.Count);

                var internalLevel2TopologyNode = internalTopologyNode2.Outgoing.First();
                var internalLevel2DeserializedTopologyNode = internalDeserializedTopologyNode2.Outgoing.First();
                AssertEqual(internalLevel2TopologyNode, internalLevel2DeserializedTopologyNode);
                Assert.Empty(internalLevel2DeserializedTopologyNode.Outgoing);
            }
        }

        [Fact]
        public void Topology_with_multilevel_tree_should_work_with_blittable()
        {
            var topologyMasterNode = new TopologyNode
            {
                Node = new ServerNode
                {
                    DbId = Guid.NewGuid().ToString(),
                    Url = "the_url",
                    ApiKey = "ABC",
                    Database = "the_database"
                },
                SpecifiedCollections = new Dictionary<string, string>
                {
                    {"AA","BB"},
                    {"CC","DD"}
                },
                Outgoing = new List<TopologyNode>
                {
                     new TopologyNode
                            {
                                Node = new ServerNode
                                {
                                    DbId = Guid.NewGuid().ToString(),
                                    Url = "the_url31",
                                    ApiKey = "ABC31",
                                    Database = "the_database31"
                                },
                                SpecifiedCollections = new Dictionary<string, string>
                                {
                                    {"ZZ2","LL2"}
                                }
                            },
                    new TopologyNode
                    {
                        Node = new ServerNode
                        {
                            DbId = Guid.NewGuid().ToString(),
                            Url = "the_url2",
                            ApiKey = "ABC2",
                            Database = "the_database2"
                        },
                        SpecifiedCollections = new Dictionary<string, string>
                        {
                            {"DD","EE"},
                            {"FF","GG"}
                        },
                        Outgoing = new List<TopologyNode>
                        {
                            new TopologyNode
                            {
                                Node = new ServerNode
                                {
                                    DbId = Guid.NewGuid().ToString(),
                                    Url = "the_url3",
                                    ApiKey = "ABC3",
                                    Database = "the_database3"
                                },
                                SpecifiedCollections = new Dictionary<string, string>
                                {
                                    {"ZZ","LL"}
                                }
                            }
                        }
                    }
                }
            };

            var topology = new Topology
            {
                Outgoing = topologyMasterNode.Outgoing,
                LeaderNode = topologyMasterNode.Node
            };

            var dynamicJson = topology.ToJson();
            using (var context = new JsonOperationContext(1024, 1024))
            using (var blittableJson = context.ReadObject(dynamicJson, string.Empty))
            {
                var deserializedTopology = JsonManualDeserialization.ConvertToTopology(blittableJson);
                AssertEqual(topology.Outgoing.First(), deserializedTopology.Outgoing.First());
                Assert.Equal(2, deserializedTopology.Outgoing.Count);

                var internalTopologyNode = deserializedTopology.Outgoing.First();
                var internalDeserializedTopologyNode = deserializedTopology.Outgoing.First();
                AssertEqual(internalTopologyNode, internalDeserializedTopologyNode);
                Assert.Empty(internalDeserializedTopologyNode.Outgoing);

                var internalTopologyNode2 = deserializedTopology.Outgoing.Skip(1).First();
                var internalDeserializedTopologyNode2 = deserializedTopology.Outgoing.Skip(1).First();
                AssertEqual(internalTopologyNode2, internalDeserializedTopologyNode2);
                Assert.Equal(1, internalDeserializedTopologyNode2.Outgoing.Count);

                var internalLevel2TopologyNode = internalTopologyNode2.Outgoing.First();
                var internalLevel2DeserializedTopologyNode = internalDeserializedTopologyNode2.Outgoing.First();
                AssertEqual(internalLevel2TopologyNode, internalLevel2DeserializedTopologyNode);
                Assert.Empty(internalLevel2DeserializedTopologyNode.Outgoing);
            }
        }

        private static void AssertEqual(TopologyNode topologyMasterNode, TopologyNode deserializedTopologyMasterNode)
        {
            Assert.Equal(topologyMasterNode.Node.Url, deserializedTopologyMasterNode.Node.Url);
            Assert.Equal(topologyMasterNode.Node.DbId, deserializedTopologyMasterNode.Node.DbId);
            Assert.Equal(topologyMasterNode.Node.Database, deserializedTopologyMasterNode.Node.Database);

            Assert.True(topologyMasterNode.SpecifiedCollections.ContentEquals(deserializedTopologyMasterNode.SpecifiedCollections));
        }

        [Fact]
        public async void TomstoneToTombstoneConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var doc = WaitForDocument(slave, "users/1");
                Assert.True(doc);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var deletedDoc = WaitForDocumentDeletion(slave, "users/1");
                Assert.True(deletedDoc);

                using (var session = master.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                bool failed = false;
                try
                {
                    await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

        [Fact]
        public async void NonIdenticalContentConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2,conflicts["users/1"].Count);
            }
        }

        [Fact]
        public async void NonIdenticalMetadataConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<ReplicationConflictsTests.User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add(("bla"), new RavenJValue("asd"));
                    session.Store(user);
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<ReplicationConflictsTests.User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add(("bla"), new RavenJValue("asd"));
                    meta.Add(("bla2"), new RavenJValue("asd"));
                    session.SaveChanges();
                }

                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }

        [Fact]
        public async void IdenticalContentConflictResolution()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                }

                
                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Age = 12,
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                bool failed = false;
                try
                {
                    await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

        [Fact]
        public async void UpdateConflictOnParentDocumentArrival()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }
                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(1, conflicts.Count);

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "Karmel123"
                    }, "users/1");
                    session.SaveChanges();
                }

                conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }
    }
}
