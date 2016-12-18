using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3435 : ReplicationBase
    {
        private const string TestDatabaseName = "testDB";
        private const string TestUsername1 = "John Doe A";
        private const string TestUsername2 = "John Doe B";
        private readonly HttpRavenRequestFactory httpRavenRequestFactory;

        public RavenDB_3435()
        {
            httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Debugger.IsAttached ? 600000 : 15000 };
        }

        public class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        public class DocumentStoreSetDateModifiedListener : IDocumentStoreListener
        {
            // This will get called after the 'DocumentConflictListener' is handled - repopulating the Asos
            // Last Modified Date property
            public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
            {
                if (metadata == null)
                {
                    // Log that the metadata was null (warning?), but assume all is well
                    return true;
                }

                var lastModifiedDate = DateTime.UtcNow;
                metadata[Constants.RavenLastModified] = lastModifiedDate;
                return true;
            }

            public void AfterStore(string key, object entityInstance, RavenJObject metadata)
            {
                // All's good in the hood
            }
        }

        public class TestDocumentConflictListener : IDocumentConflictListener
        {
            public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
            {
                if (conflictedDocs == null || !conflictedDocs.Any())
                {
                    resolvedDocument = null;
                    return false;
                }

                if (key.StartsWith("Raven/"))
                {
                    resolvedDocument = null;
                    return false;
                }

                var maxDate = conflictedDocs.Max(x => x.Metadata.Value<DateTimeOffset>(Constants.RavenLastModified));

                resolvedDocument =
                    conflictedDocs.FirstOrDefault(x => x.Metadata.Value<DateTimeOffset>(Constants.RavenLastModified) == maxDate);
                if (resolvedDocument != null)
                {
                    // Do the logging before we override the metadata
                    resolvedDocument.Metadata.Remove("@id");
                    resolvedDocument.Metadata.Remove("@etag");
                    resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflict);
                    resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                }

                return resolvedDocument != null;
            }
        }

        [Fact]
        public void Resolution_of_conflict_should_delete_all_conflict_files_with_simulated_second_node()
        {
            var user = new User
            {
                Name = TestUsername1
            };

            using (var storeB = CreateStore(databaseName: TestDatabaseName))
            {
                //initial replication -> essentially create the doc in storeB
                var initialReplicationRequestBody = RavenJArray.Parse("[{\"Max\":32,\"@metadata\":{\"Raven-Replication-Version\":2,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\",\"@id\":\"Raven/Hilo/users\",\"Last-Modified\":\"2015-05-19T11:28:05.2198563Z\",\"Raven-Last-Modified\":\"2015-05-19T11:28:05.2198563\",\"@etag\":\"01000000-0000-0002-0000-000000000003\"}},{\"Name\":\"John Doe A\",\"@metadata\":{\"Raven-Entity-Name\":\"Users\",\"Raven-Clr-Type\":\"Raven.Tests.Issues.RavenDB_3435+User, Raven.Tests.Issues\",\"Raven-Replication-Version\":3,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\",\"@id\":\"users/1\",\"Last-Modified\":\"2015-05-19T11:28:05.2348669Z\",\"Raven-Last-Modified\":\"2015-05-19T11:28:05.2348669\",\"@etag\":\"01000000-0000-0002-0000-000000000004\"}}]");
                var serverUrl = servers[0].DocumentStore.Url;
                var serverPort = servers[0].Configuration.Port;
                var url = string.Format("{0}:{1}/databases/{2}/replication/replicateDocs?from=http%3A%2F%2Fmichael%3A9000%2Fdatabases%2FtestDB&dbid=b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a&count=2", serverUrl, serverPort, TestDatabaseName);

                var replicateRequest = httpRavenRequestFactory.Create(url, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    DefaultDatabase = TestDatabaseName,
                    Url = url
                });
                replicateRequest.Write(initialReplicationRequestBody);
                replicateRequest.ExecuteRequest();				

                ChangeDocument(storeB, "users/1", TestUsername2);

                //simulate what happens when on storeA the doc is changed -> replication request to storeB
                var afterChangeReplicationRequestBody = RavenJArray.Parse("[{\"Name\":\"John Doe B2\",\"@metadata\":{\"Raven-Entity-Name\":\"Users\",\"Raven-Clr-Type\":\"Raven.Tests.Issues.RavenDB_3435+User, Raven.Tests.Issues\",\"Raven-Replication-Version\":4,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\",\"Raven-Replication-History\":[{\"Raven-Replication-Version\":3,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\"}],\"@id\":\"users/1\",\"Last-Modified\":\"2015-05-19T11:28:05.7662451Z\",\"Raven-Last-Modified\":\"2015-05-19T11:28:05.7662451\",\"@etag\":\"01000000-0000-0002-0000-000000000005\"}}]");
                url = string.Format("{0}:{1}/databases/{2}/replication/replicateDocs?from=http%3A%2F%2Fmichael%3A9000%2Fdatabases%2FtestDB&dbid=b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a&count=1", serverUrl, serverPort, TestDatabaseName);

                replicateRequest = httpRavenRequestFactory.Create(url, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    DefaultDatabase = TestDatabaseName,
                    Url = url
                });

                replicateRequest.Write(afterChangeReplicationRequestBody);
                replicateRequest.ExecuteRequest();				

                //sanity check -> make sure that the conflict is created on storeB
                Assert.True(WaitForConflictDocumentsToAppear(storeB, user.Id, TestDatabaseName));

                //simulate replication request from storeA as if concurrent replication happened on storeA and storeB
                var storeBDatabaseId = storeB.DatabaseCommands.ForDatabase(TestDatabaseName).GetStatistics().DatabaseId.ToString();
                var requestString = "[{\"Name\":\"John Doe B\",\"@metadata\":{\"Raven-Entity-Name\":\"Users\",\"Raven-Clr-Type\":\"Raven.Tests.Issues.RavenDB_3435+User, Raven.Tests.Issues\",\"Raven-Replication-Version\":2,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\",\"Raven-Replication-History\":[{\"Raven-Replication-Version\":3,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\"},{\"Raven-Replication-Version\":3,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\"},{\"Raven-Replication-Version\":4,\"Raven-Replication-Source\":\"b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a\"},{\"Raven-Replication-Version\":2,\"Raven-Replication-Source\":\"6009d0d3-4976-41e9-8068-110f97d894be\"}],\"@id\":\"users/1\",\"Last-Modified\":\"2015-05-19T11:28:07.6190254Z\",\"Raven-Last-Modified\":\"2015-05-19T11:28:07.6190254\",\"@etag\":\"01000000-0000-0003-0000-000000000005\"}}]";
                requestString = requestString.Replace("6009d0d3-4976-41e9-8068-110f97d894be", storeBDatabaseId);
                var afterConflictResolveRequestBody = RavenJArray.Parse(requestString);			
                url = string.Format("{0}:{1}/databases/{2}/replication/replicateDocs?from=http%3A%2F%2Fmichael%3A9000%2Fdatabases%2FtestDB&dbid=b2f4bdf5-9bc2-46bf-a173-8c441c5b3b5a&count=1", serverUrl, serverPort, TestDatabaseName);

                replicateRequest = httpRavenRequestFactory.Create(url, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    DefaultDatabase = TestDatabaseName,
                    Url = url
                });

                replicateRequest.Write(afterConflictResolveRequestBody);
                replicateRequest.ExecuteRequest();

                Assert.True(CheckIfConflictDocumentsIsThere(storeB, "users/1", TestDatabaseName));
            }
        }

        private static void ChangeDocument(DocumentStore store, string id, string newName)
        {
            using (var session = store.OpenSession())
            {
                var fetchedUser = session.Load<User>(id);				
                fetchedUser.Name = newName;
                session.SaveChanges();
            }
        }
    }
}
