
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Xunit;
using Raven.Client.Linq;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    public class LazilyLoadWithTransformerWhileUsingSharding : RavenTest
    {
        private new readonly Dictionary<string, RavenDbServer> servers;
        private readonly ShardedDocumentStore store;
        private Dictionary<string, IDocumentStore> documentStores;

        public LazilyLoadWithTransformerWhileUsingSharding()
        {
            servers = new Dictionary<string, RavenDbServer>
            {
                {"shard", GetNewServer(8079)}
            };

            documentStores = new Dictionary<string, IDocumentStore>
            {
                {"shard", new DocumentStore{Url = "http://localhost:8079"}}
            };

            foreach (var documentStore in documentStores)
            {
                documentStore.Value.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
            }


            var shardStrategy = new ShardStrategy(documentStores);

            store = new ShardedDocumentStore(shardStrategy);
            store.Initialize();

            new TestTransformer().Execute(store);
            new TestTransformerFromStoredIndex().Execute(store);
            new TestWithStorageIndex().Execute(store);
            new TestIndex().Execute(store);
            new TestReduceIndex().Execute(store);
        }

        [Fact]
        public void LazyLoadWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument();
                session.Store(testDoc);

                session.SaveChanges();

                var result = session.Advanced.Lazily.Load<TestTransformer, TestDto>(testDoc.Id);
                Assert.NotNull(result.Value);
            }
        }

        [Fact]
        public void LoadWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument();
                session.Store(testDoc);

                session.SaveChanges();

                var result = session.Load<TestTransformer, TestDto>(testDoc.Id);
                Assert.NotNull(result);
            }
        }

        [Fact]
        public void QueryWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument { Id = "test_doc" };
                session.Store(testDoc);
                session.SaveChanges();

                var result = session.Query<TestDocument>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id.Equals("shard/test_doc"))
                    .TransformWith<TestTransformer, TestDto>();

                Assert.NotNull(result.FirstOrDefault());
            }
        }


        [Fact]
        public void QueryOnStoringIndexWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument { Id = "test_doc" };
                session.Store(testDoc);
                session.SaveChanges();

                var result = session.Query<CustomIndexObject, TestWithStorageIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id.Equals("shard/test_doc"))
                    .TransformWith<TestTransformerFromStoredIndex, TestDto>();

                Assert.NotNull(result.FirstOrDefault());
            }
        }


        [Fact]
        public void QueryLazilyOnStoringIndexWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument { Id = "test_doc" };
                session.Store(testDoc);
                session.SaveChanges();

                var result = session.Query<CustomIndexObject, TestWithStorageIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id.Equals("shard/test_doc"))
                    .TransformWith<TestTransformerFromStoredIndex, TestDto>()
                    .Lazily();
                var res = result.Value.FirstOrDefault();
                Assert.NotNull(res);
            }
        }

        [Fact]
        public void QueryOnIndexWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument { Id = "test_doc" };
                session.Store(testDoc);
                session.SaveChanges();

                var result = session.Query<TestDocument, TestIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id.Equals("shard/test_doc"))
                    .TransformWith<TestTransformer, TestDto>();

                Assert.NotNull(result.FirstOrDefault());
            }
        }

        [Fact]
        public void QueryOnReducedIndexWithTransformerInShardedSetup()
        {
            using (var session = store.OpenSession())
            {
                var testDoc = new TestDocument { Id = "test_doc" };
                session.Store(testDoc);
                session.SaveChanges();

                var result = session.Query<TestReduceIndex.ReduceResult, TestReduceIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id.Equals("shard/test_doc"))
                    .TransformWith<TestTransformer, TestDto>()
                    .FirstOrDefault();

                Assert.NotNull(result);
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
        }

        public class TestDto
        {
            public string Test { get; set; }
        }

        public class CustomIndexObject
        {
            public string Id { get; set; }
            public string NewType { get; set; }
        }

        public class TestTransformer : AbstractTransformerCreationTask<TestDocument>
        {
            public TestTransformer()
            {
                TransformResults = contacts => from c in contacts
                                               select new
                                               {
                                                   Test = "test"
                                               };
            }
        }


        public class TestTransformerFromStoredIndex : AbstractTransformerCreationTask<CustomIndexObject>
        {
            public TestTransformerFromStoredIndex()
            {
                TransformResults = contacts => from c in contacts
                                               select new TestDto
                                               {
                                                   Test = c.NewType
                                               };
            }
        }

        public class TestIndex : AbstractIndexCreationTask<TestDocument>
        {
            public TestIndex()
            {
                Map = documents =>
                    from document in documents
                    select new
                    {
                        document.Id
                    };
            }
        }

        public class TestWithStorageIndex : AbstractIndexCreationTask<TestDocument>
        {
            public TestWithStorageIndex()
            {
                Map = documents =>
                    from document in documents
                    select new
                    {
                        Id = document.Id,
                        NewType = "newType"
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }


        public class TestReduceIndex : AbstractIndexCreationTask<TestDocument, TestReduceIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string ForeignProperty { get; set; }
            }

            public TestReduceIndex()
            {
                Map = documents =>
                    from document in documents
                    select new ReduceResult
                    {
                        Id = document.Id,
                        ForeignProperty = "test"
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Id
                        into g
                        select new ReduceResult
                        {
                            Id = g.Key,
                            ForeignProperty = "reduced"
                        };
            }

            protected internal override IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
            {
                return enumerable;
            }
        }

        public override void Dispose()
        {
            foreach (var ravenDbServer in servers)
            {
                ravenDbServer.Value.Dispose();
            }
            store.Dispose();
            base.Dispose();
        }

    }
}
