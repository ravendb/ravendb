// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4153.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4153 : RavenTest
    {
        const string IndexName = "test";

        [Theory]
        [PropertyData("Storages")]
        public void can_change_reduce_key_leaving_correct_stats1(string storageType)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: storageType))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{ Company = "companies/1"});
                    session.SaveChanges();
                }

                CreateIndex(store);
                WaitForIndexing(store);

                var json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                var results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(1, results[0].Count);
                Assert.Equal("companies/1", results[0].Key);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.Company = "companies/2";
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(1, results[0].Count);
                Assert.Equal("companies/2", results[0].Key);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.Company = "companies/3";
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(1, results[0].Count);
                Assert.Equal("companies/3", results[0].Key);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.Company = null;
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(0, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(0, results.Count);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_change_reduce_key_leaving_correct_stats2(string storageType)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: storageType))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{ Company = "companies/1"});
                    session.SaveChanges();
                }

                CreateIndex(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var json = GetIndexKeysStats(store);
                Assert.Equal(0, json["Count"]);
                var results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(0, results.Count);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_change_reduce_key_leaving_correct_stats3(string storageType)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: storageType))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Company = "companies/1" });
                    session.Store(new Order { Company = "companies/1" });
                    session.Store(new Order { Company = "companies/1" });
                    session.SaveChanges();
                }

                CreateIndex(store);
                WaitForIndexing(store);

                var json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                var results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(3, results[0].Count);
                Assert.Equal("companies/1", results[0].Key);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/2");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(2, results[0].Count);
                Assert.Equal("companies/1", results[0].Key);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/3");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(1, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(1, results.Count);
                Assert.Equal(1, results[0].Count);
                Assert.Equal("companies/1", results[0].Key);

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                json = GetIndexKeysStats(store);
                Assert.Equal(0, json["Count"]);
                results = ((RavenJArray)json["Results"])
                    .Deserialize<List<ReduceKeyAndCount>>(store.Conventions);
                Assert.Equal(0, results.Count);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void updating_performed_reduce_type_shouldnt_update_if_reduce_key_removed1(string storageType)
        {
            const int a = 100;
            using (var storage = NewTransactionalStorage(requestedStorage: storageType))
            {
                storage.Batch(accessor =>
                {
                    accessor.Indexing.AddIndex(a, true);

                    accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/3", "b", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/4", "b", new RavenJObject());

                    accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
                    accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);
                });

                storage.Batch(accessor =>
                {
                    accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.SingleStep);
                    accessor.MapReduce.UpdatePerformedReduceType(a, "b", ReduceType.SingleStep);
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.SingleStep, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.SingleStep, result);
                });

                storage.Batch(accessor => accessor.Indexing.DeleteIndex(a, new CancellationToken()));

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.None, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.None, result);
                });

                storage.Batch(accessor =>
                {
                    accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.SingleStep);
                    accessor.MapReduce.UpdatePerformedReduceType(a, "b", ReduceType.SingleStep);
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.None, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.None, result);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void updating_performed_reduce_type_shouldnt_update_if_reduce_key_removed2(string storageType)
        {
            const int a = 100;
            using (var storage = NewTransactionalStorage(requestedStorage: storageType))
            {
                storage.Batch(accessor =>
                {
                    accessor.Indexing.AddIndex(a, true);

                    accessor.MapReduce.PutMappedResult(a, "a/1", "a", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/2", "a", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/3", "b", new RavenJObject());
                    accessor.MapReduce.PutMappedResult(a, "a/4", "b", new RavenJObject());

                    accessor.MapReduce.IncrementReduceKeyCounter(a, "a", 2);
                    accessor.MapReduce.IncrementReduceKeyCounter(a, "b", 2);
                });

                storage.Batch(accessor =>
                {
                    accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.MultiStep);
                    accessor.MapReduce.UpdatePerformedReduceType(a, "b", ReduceType.SingleStep);
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.MultiStep, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.SingleStep, result);
                });

                storage.Batch(accessor =>
                {
                    var removed = new Dictionary<ReduceKeyAndBucket, int>();
                    accessor.MapReduce.DeleteMappedResultsForDocumentId("a/3", a, removed);
                    accessor.MapReduce.DeleteMappedResultsForDocumentId("a/4", a, removed);
                    accessor.MapReduce.UpdateRemovedMapReduceStats(a, removed, CancellationToken.None);

                    var reduceKeys = removed.Keys;
                    foreach (var reduceKey in reduceKeys)
                    {
                        accessor.MapReduce.UpdatePerformedReduceType(a, reduceKey.ReduceKey, 
                            ReduceType.SingleStep, skipAdd: true);
                    }
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.MultiStep, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.None, result);
                });

                storage.Batch(accessor =>
                {
                    var removed = new Dictionary<ReduceKeyAndBucket, int>();
                    accessor.MapReduce.DeleteMappedResultsForDocumentId("a/1", a, removed);
                    accessor.MapReduce.DeleteMappedResultsForDocumentId("a/2", a, removed);
                    accessor.MapReduce.UpdateRemovedMapReduceStats(a, removed, CancellationToken.None);

                    var reduceKeys = removed.Keys;
                    foreach (var reduceKey in reduceKeys)
                    {
                        accessor.MapReduce.UpdatePerformedReduceType(a, reduceKey.ReduceKey,
                            ReduceType.SingleStep, skipAdd: true);
                    }
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.None, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.None, result);
                });

                storage.Batch(accessor =>
                {
                    accessor.MapReduce.UpdatePerformedReduceType(a, "a", ReduceType.SingleStep);
                    accessor.MapReduce.UpdatePerformedReduceType(a, "b", ReduceType.SingleStep);
                });

                storage.Batch(accessor =>
                {
                    var result = accessor.MapReduce.GetLastPerformedReduceType(a, "a");
                    Assert.Equal(ReduceType.None, result);

                    result = accessor.MapReduce.GetLastPerformedReduceType(a, "b");
                    Assert.Equal(ReduceType.None, result);
                });
            }
        }

        private static void CreateIndex(DocumentStore store)
        {
            store.DatabaseCommands.PutIndex(IndexName, new IndexDefinition
            {
                Name = IndexName,
                Map = @"from order in docs.Orders
select new {order.Company,
Count = 1
}",
                Reduce = @"from result in results 
group result by result.Company into g 
select new {
Company = g.Key,
Count = g.Sum(x => x.Count)
}"
            });
        }

        private static RavenJObject GetIndexKeysStats(DocumentStore store)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + string.Format("/databases/{0}/indexes/{1}?debug=keys", store.DefaultDatabase, IndexName),
                    HttpMethod.Get, 
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var json = (RavenJObject)request.ReadResponseJson();
            return json;
        }

        public class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
        }
    }
}