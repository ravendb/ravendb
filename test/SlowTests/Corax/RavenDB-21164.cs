using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class RavenDB_21164 : RavenTestBase
    {
        public RavenDB_21164(ITestOutputHelper output) : base(output)
        {
        }

        internal class Product
        {
            public string Name;
        }

        internal class Products_ByName : AbstractIndexCreationTask<Product>
        {
            public Products_ByName()
            {
                Map = product => from u in product select new { Name = u.Name };

                Indexes.Add(x => x.Name, FieldIndexing.Search);

                IndexSuggestions.Add(x => x.Name);

                Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

                Stores.Add(x => x.Name, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetSuggestionsWithDifferentFirstLetter(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Products_ByName();
                index.Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new Product { Name = "education" }, "product/1");
                    s.Store(new Product { Name = "duration" }, "product/2");
                    s.Store(new Product { Name = "medication" }, "product/3");
                    s.Store(new Product { Name = "animation" }, "product/4");
                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<Product, Products_ByName>()
                            .SuggestUsing(x => x.ByField(y => y.Name, "aducation").WithOptions(new SuggestionOptions()))
                            .Execute();

                    Assert.Equal(4, suggestions["Name"].Suggestions.Count);
                }
            }
        }
    }
}
