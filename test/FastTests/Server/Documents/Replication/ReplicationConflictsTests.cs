using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Sparrow.Json;
using Xunit;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationConflictsTests : ReplicationTestsBase
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void All_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 11 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 12 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void All_local_etags_lower_than_remote_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status_with_different_order()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_larger_size_than_local_should_return_Update_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 40 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_with_different_dbId_set_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() };
            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 10 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_all_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_change_vector_smaller_than_local_and_some_remote_etags_higher_than_local_should_return_Conflict_at_conflict_status()
        {
            var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var local = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 3000 },
                new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
            };

            var remote = new[]
            {
                new ChangeVectorEntry { DbId = dbIds[0], Etag = 100 },
                new ChangeVectorEntry { DbId = dbIds[1], Etag = 200 },
                new ChangeVectorEntry { DbId = dbIds[2], Etag = 300 }
            };

            Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
        }


        [Fact]
        public async Task Conflict_same_time_with_master_slave()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                var conflicts = await WaitUntilHasConflict(store2, "foo/bar");
                Assert.Equal(2, conflicts["foo/bar"].Count);
            }
        }        
        
        [Fact]
        public async Task Conflict_then_data_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
        }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);
                
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);
                
                await WaitUntilHasConflict(store2, "foo/bar");
                // /indexes/Raven/DocumentsByEntityName
                //TODO: this needs to be replaced by ClientAPI LoadDocument() when the ClientAPI is finished
                var url = $"{store2.Url}/databases/{store2.DefaultDatabase}/queries/{userIndex.IndexName}";
                using (var request = store2.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {
                    var ex = Assert.Throws<ErrorResponseException>(() => request.ExecuteRequest());
                    Assert409Response(ex);
                }          
            }
        }

        [Fact]
        public async Task Conflict_then_delete_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");


                var op = store2.DatabaseCommands.DeleteByIndex(userIndex.IndexName, new IndexQuery
                {
                    Query = String.Empty
                });

                Assert.Throws<DocumentInConflictException>(() => op.WaitForCompletion());
            }
        }

        //TODO: this probably needs to be refactored when operations related functionality is finished
        protected async Task AssertOperationFaultsAsync(DocumentStore store,int operationId)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/operations/status?id={operationId}";
            var sw = Stopwatch.StartNew();
            RavenJToken response = null;
            while (sw.ElapsedMilliseconds < 10000 || response?.Value<string>("Status") != "Faulted")
            {
                using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, HttpMethod.Get,
                        new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {
                    response = await request.ReadResponseJsonAsync();
                }
            }            
            Assert.NotNull(response); //precaution
            Assert.Equal("Faulted", response.Value<string>("Status"));

            var result = response.Value<RavenJToken>("Result");            
            Assert.Contains("DocumentConflictException", result.Value<string>("StackTrace"));
        }

        [Fact]
        public async Task Conflict_then_patching_query_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                var userIndex = new UserIndex();
                store2.ExecuteIndex(userIndex);

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }
                WaitForIndexing(store2);
                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");

           
                // /indexes/Raven/DocumentsByEntityName
                var op = store2.DatabaseCommands.UpdateByIndex(userIndex.IndexName, new IndexQuery
                {
                    Query = String.Empty
                }, new Raven.Client.Data.PatchRequest
                {
                    Script = String.Empty
                });

                Assert.Throws<DocumentInConflictException>(() => op.WaitForCompletion());
            }
        }

        [Fact]
        public async Task Conflict_then_load_by_id_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");

                //TODO: this needs to be replaced by ClientAPI LoadDocument() when the ClientAPI is finished
                var url = $"{store2.Url}/databases/{store2.DefaultDatabase}/docs?id=foo/bar";
                using (var request = store2.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {
                    var ex = Assert.Throws<ErrorResponseException>(() => request.ExecuteRequest());
                    Assert409Response(ex);
                }
            }
        }

        [Fact]
        public async Task Conflict_then_put_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");

                //TODO: this needs to be replaced by ClientAPI Delete() when the ClientAPI is finished
                var url = $"{store2.Url}/databases/{store2.DefaultDatabase}/docs?id=foo/bar";
                using (var request = store2.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, HttpMethod.Put, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {                     
                    var ex = Assert.Throws<AggregateException>(() => request.WriteWithObjectAsync(new User()).Wait());
                    Assert409Response(ex.InnerException);
                }
            }
        }

        [Fact]
        public async Task Conflict_then_patch_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");

                //TODO: this needs to be replaced by ClientAPI Delete() when the ClientAPI is finished
                var url = $"{store2.Url}/databases/{store2.DefaultDatabase}/docs?id=foo/bar";
                using (var request = store2.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, new HttpMethod("PATCH"), new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {
                    var ex = Assert.Throws<AggregateException>(() => request.WriteWithObjectAsync(new
                    {
                        Patch = new PatchRequest
                        {
                            Script = "this.x = 123"
                        }
                    }).Wait());
                    Assert409Response(ex.InnerException);
                }
            }
        }

        [Fact] public async Task Conflict_then_delete_request_will_return_409_and_conflict_data()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                SetupReplication(store1, store2);

                await WaitUntilHasConflict(store2, "foo/bar");

                //TODO: this needs to be replaced by ClientAPI Delete() when the ClientAPI is finished
                var url = $"{store2.Url}/databases/{store2.DefaultDatabase}/docs?id=foo/bar";
                using (var request = store2.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, url, HttpMethod.Delete, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
                {
                    var ex = Assert.Throws<ErrorResponseException>(() => request.ExecuteRequest());
                    Assert409Response(ex);
                }
            }
        }

        private static void Assert409Response(Exception e)
        {
            var theException = e as ErrorResponseException;
            Assert.NotNull(theException);
            Assert.Equal(HttpStatusCode.Conflict, theException.StatusCode);
            var responseJson = RavenJObject.Parse(theException.ResponseString);
            Assert.Equal("foo/bar", responseJson.Value<RavenJToken>("ConflictInfo").Value<string>("DocId"));
        }


        [Fact]
        public async Task Conflict_should_work_on_master_slave_slave()
        {
            var dbName1 = "FooBar-1";
            var dbName2 = "FooBar-2";
            var dbName3 = "FooBar-3";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }
                using (var s3 = store3.OpenSession())
                {
                    s3.Store(new User(), "foo/bar");
                    s3.SaveChanges();
                }

                SetupReplication(store1, store3);
                SetupReplication(store2, store3);

                var conflicts = await WaitUntilHasConflict(store3, "foo/bar", 3);

                Assert.Equal(3, conflicts["foo/bar"].Count);
            }
        }	

        private async Task<Dictionary<string, List<ChangeVectorEntry[]>>> WaitUntilHasConflict(
                DocumentStore store,
                string docId,
                int count = 1,
                int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;
            Dictionary<string, List<ChangeVectorEntry[]>> conflicts;
            var sw = Stopwatch.StartNew();
            do
            {
                conflicts = await GetConflicts(store, docId);

                List<ChangeVectorEntry[]> list;
                if (conflicts.TryGetValue(docId, out list) == false)
                    list = new List<ChangeVectorEntry[]>();
                if (list.Count >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    Assert.False(true,
                        "Timed out while waiting for conflicts on " + docId + " we have " + list.Count + " conflicts");
                }

            } while (true);
            return conflicts;
        }

        private async Task<Dictionary<string, List<ChangeVectorEntry[]>>> GetConflicts(DocumentStore store,
            string docId)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/replication/conflicts?docId={docId}";
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
            {
                request.ExecuteRequest();
                var conflictsJson = RavenJArray.Parse(await request.Response.Content.ReadAsStringAsync());
              
                var conflicts = conflictsJson.Select(x => new
                {
                    Key = x.Value<string>("Key"),
                    ChangeVector = x.Value<RavenJArray>("ChangeVector").Select(c => c.FromJson()).ToArray()
                }).GroupBy(x => x.Key).ToDictionary(x => x.Key, x => x.Select(i => i.ChangeVector).ToList());

                return conflicts;
            }
        }

        public class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from u in users
                    select new User
                    {
                        Id = u.Id,
                        Name = u.Name,
                        Age = u.Age
                    };

                Index(x => x.Name, FieldIndexing.Analyzed);

                Analyze(x => x.Name, typeof(RavenStandardAnalyzer).FullName);

            }
        }
    }
}

