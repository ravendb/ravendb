using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.BR;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4836 : RavenTestBase
    {
        [Fact]
        public void MoreLikeThisQueryDefaultAnalyzer()
        {
            using (var store = NewDocumentStore())
            {
                store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(MyAnalyzerGenerator)));
                store.ExecuteIndex(new Posts_Index());

                using (var session = store.OpenSession())
                {
                    var dataQueriedFor = new Post { Id = "posts/123", Body = "Isto é um teste. Não é fixe? Espero que o teste passe!" };

                    var someData = new List<Post>
                {
                    dataQueriedFor,
                    new Post { Id = "posts/234", Body = "Tenho um teste amanhã. Detesto ter testes" },
                    new Post { Id = "posts/3456", Body = "Bolo é espetacular" },
                    new Post { Id = "posts/3457", Body = "Este document só tem a palavra teste uma vez" },
                    new Post { Id = "posts/3458", Body = "teste", },
                    new Post { Id = "posts/3459", Body = "testes", }
                };
                    someData.ForEach(session.Store);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Advanced
                        .MoreLikeThis<Post, Posts_Index>(new MoreLikeThisQuery
                        {
                            DocumentId = "posts/123",
                            Fields = new[] { "Body" },
                            DefaultAnalyzerName = typeof(BrazilianAnalyzer).AssemblyQualifiedName
                        }).ToList());
                }
            }
        }
    }

    class MyAnalyzerGenerator : AbstractAnalyzerGenerator
    {
        public override Analyzer GenerateAnalyzerForIndexing(string indexName, Document document, Analyzer previousAnalyzer)
        {
            return DefaultAnalyzer(previousAnalyzer);
        }

        public override Analyzer GenerateAnalyzerForQuerying(string indexName, string query, Analyzer previousAnalyzer)
        {
            return DefaultAnalyzer(previousAnalyzer);
        }

        private static Analyzer DefaultAnalyzer(Analyzer previousAnalyzer)
        {
            var perFieldAnalyzerWrapper = previousAnalyzer as RavenPerFieldAnalyzerWrapper;
            if (perFieldAnalyzerWrapper != null)
            {
                perFieldAnalyzerWrapper.AddAnalyzer("Body", new BrazilianAnalyzer(Lucene.Net.Util.Version.LUCENE_30));
            }

            return previousAnalyzer;
        }
    }

    class Post
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Body { get; set; }
    }

    class Posts_Index : AbstractIndexCreationTask<Post>
    {
        public Posts_Index()
        {
            Map = posts => from post in posts
                           select new
                           {
                               post.Id,
                               post.Title,
                               post.Body
                           };

            Stores.Add(x => x.Body, FieldStorage.Yes);
        }
    }
}
