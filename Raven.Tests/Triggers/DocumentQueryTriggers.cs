using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Triggers
{
    public class DocumentQueryTriggers : RavenTest
    {
        [Fact]
        public void ExecuteDocumentQueryTriggersOnFacets()
        {
            using (var store = NewDocumentStore())
            {
                store.RegisterListener(new CustomDocumentQueryListener());
                store.ExecuteIndex(new Custom_Index());

                using (var session = store.OpenSession())
                {
                    session.Store(new CustomObject { Text = "Testing" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session
                        .Query<CustomObject, Custom_Index>()
                        .Where(x => x.Text == "error")
                        .Take(1)
                        .FirstOrDefault();
                    Assert.NotNull(result);

                    var facets = session
                        .Query<CustomObject, Custom_Index>()
                        .Where(x => x.Text == "error")
                        .ToFacets(new List<Facet> { new Facet<CustomObject> { Name = x => x.Text } });
                    Assert.Equal(1, facets.Results.Values.First().Values.First().Hits);
                }
            }
        }

        [Fact]
        public void ExecuteDocumentQueryTriggersOnStreams()
        {
            using (var store = NewDocumentStore())
            {
                store.RegisterListener(new CustomDocumentQueryListener());
                store.ExecuteIndex(new Custom_Index());

                using (var session = store.OpenSession())
                {
                    session.Store(new CustomObject { Text = "Testing" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session
                        .Query<CustomObject, Custom_Index>()
                        .Where(x => x.Text == "error")
                        .Take(1)
                        .FirstOrDefault();
                    Assert.NotNull(result);

                    var query = session
                        .Query<CustomObject, Custom_Index>()
                        .Where(x => x.Text == "error");
                    var stream = session.Advanced.Stream(query);
                    Assert.True(stream.MoveNext());
                    Assert.Equal(result, stream.Current.Document);
                }
            }
        }

        class CustomObject
        {
            public string Id { get; set; }
            public string Text { get; set; }
        }

        class CustomDocumentQueryListener : IDocumentQueryListener
        {
            public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
            {
                queryCustomization.BeforeQueryExecution(q =>
                {
                    q.Query = "Text: Testing";
                });
            }
        }

        class Custom_Index : AbstractIndexCreationTask<CustomObject>
        {
            public Custom_Index()
            {
                Map = results =>
                    from result in results
                    select new CustomObject
                    {
                        Id = result.Id,
                        Text = result.Text
                    };
            }
        }
    }
}
