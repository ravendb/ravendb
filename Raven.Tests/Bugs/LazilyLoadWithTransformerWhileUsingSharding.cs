
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class LazilyLoadWithTransformerWhileUsingSharding : RavenTest
    {
        private readonly Dictionary<string, RavenDbServer> servers;
        private readonly ShardedDocumentStore store;
        private Dictionary<string, IDocumentStore> documentStores;

        public LazilyLoadWithTransformerWhileUsingSharding()
        {
            servers = new Dictionary<string, RavenDbServer>
			{
				{"shard", GetNewServer(8078)}
			};

            documentStores = new Dictionary<string, IDocumentStore>
			{
				{"shard", new DocumentStore{Url = "http://localhost:8078"}}
			};

            foreach (var documentStore in documentStores)
            {
                documentStore.Value.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
            }


            var shardStrategy = new ShardStrategy(documentStores);

            store = new ShardedDocumentStore(shardStrategy);
            store.Initialize();

            new TestTransformer().Execute(store);
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

        public class TestDocument
        {
            public string Id { get; set; }
        }

        public class TestDto
        {
            public string Test { get; set; }
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