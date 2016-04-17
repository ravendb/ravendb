// -----------------------------------------------------------------------
//  <copyright file="ShardedSessionInclude.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client;
using Raven.Client.Shard;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Shard
{
    public class ShardedSessionInclude : RavenTest
    {
        private new readonly RavenDbServer[] servers;
        private readonly ShardedDocumentStore shardedDocumentStore;
        private readonly IDocumentStore documentStore;

        public ShardedSessionInclude()
        {
            servers = new[]
            {
                GetNewServer(8079,requestedStorage: "esent"),
                GetNewServer(8080, requestedStorage: "esent")
            };

            documentStore = CreateDocumentStore(8080);
            shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
            {
                {"1", CreateDocumentStore(8079)}
            }));
            shardedDocumentStore.Initialize();
            documentStore.Initialize();
        }

        private static IDocumentStore CreateDocumentStore(int port)
        {
            return new DocumentStore
            {
                Url = string.Format("http://localhost:{0}/", port),
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };
        }

        [Fact]
        public void VerifyRequestCountUsingIncludeWithShardedDocumentstore()
        {
            var doc1 = new Document() { Id = "Document1" };
            var doc2 = new Document() { Id = "Document2" };
            using (var session = shardedDocumentStore.OpenSession())
            {
                session.Store(doc1);
                session.Store(doc2);
                session.Store(new DocumentContainer() { Id = "documentcontainer1", Documents = { doc1, doc2 } });
                //Assert.Equal(doc1.Name, session.Advanced.GetDocumentId(doc1));

                session.SaveChanges();
            }

            using (var session = shardedDocumentStore.OpenSession())
            {
                var companies = session.Query<DocumentContainer>()
                    .Include<DocumentContainer>(dc => dc.Documents.Select(dl => dl.Id))
                    .ToArray();
                var initialReqCount = session.Advanced.NumberOfRequests;
                var doc = session.Load<Document>(doc1.Id);
                var secondRecCount = session.Advanced.NumberOfRequests;

                Assert.Equal(initialReqCount, secondRecCount);
            }
        }

        [Fact]
        public void VerifyRequestCountUsingIncludeWithLoadIEnumerableWithShardedDocumentstore()
        {
            var doc1 = new Document() { Id = "Document1" };
            var doc2 = new Document() { Id = "Document2" };
            using (var session = shardedDocumentStore.OpenSession())
            {
                session.Store(doc1);
                session.Store(doc2);
                session.Store(new DocumentContainer()
                {
                    Id = "documentcontainer1",

                    Documents = { doc1, doc2 }
                });

                session.SaveChanges();
            }

            using (var session = shardedDocumentStore.OpenSession())
            {
                var companies = session.Query<DocumentContainer>()
                    .Include<DocumentContainer>(dc => dc.Documents.Select(dl => dl.Id))
                    .ToArray();
                var initialReqCount = session.Advanced.NumberOfRequests;
                var docs = session.Load<Document>(new List<string>() { doc1.Id, doc2.Id });
                var secondRecCount = session.Advanced.NumberOfRequests;

                Assert.NotNull(docs);
                Assert.NotEmpty(docs);
                Assert.Equal(initialReqCount, secondRecCount);
            }
        }

        [Fact]
        public void VerifyRequestCountUsingIncludeWithDocumentstore()
        {
            var doc1 = new Document() { Id = "Document1" };
            var doc2 = new Document() { Id = "Document2" };
            using (var session = documentStore.OpenSession())
            {
                session.Store(doc1);
                session.Store(doc2);
                session.Store(new DocumentContainer() { Id = "documentcontainer1", Documents = { doc1, doc2 } });
                //Assert.Equal(doc1.Name, session.Advanced.GetDocumentId(doc1));
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var companies = session.Query<DocumentContainer>()
                    .Include<DocumentContainer>(dc => dc.Documents.Select(dl => dl.Id))
                    .ToArray();
                var initialReqCount = session.Advanced.NumberOfRequests;
                var doc = session.Load<Document>(doc1.Id);
                var secondRecCount = session.Advanced.NumberOfRequests;

                Assert.Equal(initialReqCount, secondRecCount);
            }
        }

        public override void Dispose()
        {
            documentStore.Dispose();
            foreach (var server in servers)
            {
                server.Dispose();
            }
            base.Dispose();
        }

        public class DocumentContainer
        {
            public List<Document> Documents { get; set; }
            public string Id { get; set; }
            public DocumentContainer()
            {
                Documents = new List<Document>();
            }
        }

        public class Document
        {
            public string Id { get; set; }
        }
    }
}