using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using SlowTests.Utils.Analyzers;
using Xunit;
using Enumerable = System.Linq.Enumerable;

namespace SlowTests.MailingList.Stacey
{
    public class InServerTesting : RavenTestBase
    {
        private class Image
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ICollection<string> Users { get; set; }
            public ICollection<string> Tags { get; set; }
        }

        private class ImageByName : AbstractIndexCreationTask<Image, ImageByName.ReduceResult>
        {

            public class ReduceResult
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public ImageByName()
            {
                Map = docs => from i in docs
                              select new
                              {
                                  Id = i.Id,
                                  Name = new[] { i.Name },
                              };
                Index(r => r.Name, FieldIndexing.Analyzed);
                Analyzers.Add(n => n.Name, typeof(NGramAnalyzer).AssemblyQualifiedName);
            }
        }

        [Fact]
        public void ngram_search_not_empty()
        {
            using (var store = GetDocumentStore())
            {
                new ImageByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
                    session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var images = Enumerable.ToList<Image>(session.Query<Image, ImageByName>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .OrderBy(x => x.Name)
                                        .Search(x => x.Name, "phot"));

                    Assert.NotEmpty(images);
                }
            }
        }
    }
}
