using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Shard
{
    public class ShardedTransformed : RavenTest
    {
        private new readonly RavenDbServer[] servers;
        private readonly IDocumentStore store;

        public ShardedTransformed()
        {
            servers = new[]
            {
                GetNewServer(8079, requestedStorage: "esent"),
                GetNewServer(8078, requestedStorage: "esent")
            };

            Dictionary<string, IDocumentStore> shards = new Dictionary<string, IDocumentStore>
            {
                {"shard1", new DocumentStore {Url = "http://localhost:8079"}},
                {"shard2", new DocumentStore {Url = "http://localhost:8078"}}
            };

            var shardStrategy = new ShardStrategy(shards);
            store = new ShardedDocumentStore(shardStrategy).Initialize();
            new Articles_Search().Execute(store);
            new ArticlesFullTransformer().Execute(store);
        }

        [Fact]
        public void ShrededWithDocumentQueryWithTransdormer()
        {
            var doc1 = new ArticleModel { Id = "Document1", Code = "Blue", Description = "Priority 0" };
            var doc2 = new ArticleModel { Id = "Document2", Code = "Red", Description = "Priority 1" };

            using (var session = store.OpenSession())
            {
                session.Store(doc1);
                session.Store(doc2);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var docs = session.Advanced.DocumentQuery<ArticleModel>("Articles/Search")
                    .WaitForNonStaleResults()
                    .WhereEquals("Code", "Red")
                    .SetResultTransformer<ArticlesFullTransformer, ArticlesFullTransformer.ArticleModelTransformed>()
                    .ToList();
                Assert.Equal("Red", docs.Single().Code);
            }
        }

        public class Articles_Search : AbstractIndexCreationTask<ArticleModel, ArticleModel>
        {
            public Articles_Search()
            {
                Map = articles =>
                    from article in articles
                    select new
                    {
                        ClientId = article.ClientId,
                        Id = article.Id,
                        Code = article.Code,
                        Description = article.Description
                    };
            }


        }

        public class ArticleModel
        {
            public string ClientId { get; set; }
            public string Id { get; set; }
            public string Code { get; set; }
            public string Description { get; set; }
        }

        public class ArticlesFullTransformer : AbstractTransformerCreationTask<ArticleModel>
        {
            public class ArticleModelTransformed
            {
                public string Code { get; set; }
                public string Description { get; set; }
            }

            public ArticlesFullTransformer()
            {
                TransformResults = models => from model in models
                                             select new ArticleModelTransformed
                                             {
                                                 Code = model.Code,
                                                 Description = model.Description,
                                             };
            }
        }

        public override void Dispose()
        {
            store.Dispose();
            foreach (var server in servers)
            {
                server.Dispose();
            }
            base.Dispose();
        }
    }
}
