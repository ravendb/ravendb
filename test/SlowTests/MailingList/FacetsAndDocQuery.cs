using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.TestDriver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class FacetsAndDocQuery : RavenTestBase
    {
        public FacetsAndDocQuery(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentQueryWithFacetsInOneQueryTest()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Person_ForSearch());
                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Person
                        {
                            Id = Guid.NewGuid().ToString(),
                            Name = "Jan",
                            Age = 18,
                            Document = "Hi",
                            DocumentId = "240638ce-be95-4fbd-89ad-1f143f2a427a",
                        }
                    );
                    session.Store(
                        new Person
                        {
                            Id = Guid.NewGuid().ToString(),
                            Name = "Bob",
                            Age = 40,
                            Document = 213,
                            DocumentId = "92c9a02a-9401-4585-8a1a-a08a3beb4f8c",
                        }
                    );
                    session.Store(
                        new Person
                        {
                            Id = Guid.NewGuid().ToString(),
                            Name = "Jan",
                            Age = 1,
                            DocumentId = "76990947-16ed-42c9-88cc-79c153f3c899",
                        }
                    );

                    session.Store(
                        new Document
                        {
                            Id = "240638ce-be95-4fbd-89ad-1f143f2a427a",
                            Content = "Hi",
                        }
                    );
                    session.Store(
                        new Document
                        {
                            Id = "92c9a02a-9401-4585-8a1a-a08a3beb4f8c",
                            Content = 213,
                        }
                    );
                    session.Store(
                        new Document
                        {
                            Id = "76990947-16ed-42c9-88cc-79c153f3c899",
                            Content = "A random text",
                        }
                    );

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var documentQuery = session.Advanced.DocumentQuery<Person_ForSearch.Result, Person_ForSearch>()
                                        .UsingDefaultOperator(QueryOperator.And);

                    documentQuery.AndAlso().WhereEquals("Name", "Jan", false);

                    var finalQuery = documentQuery.SelectFields<Person>();

                    var lazyResults = finalQuery.Include(r => r.DocumentId).Skip(0).Take(10).Statistics(out var info)
                                    .Lazily((results) => results.ToList().ForEach((r) => r.Document = r.Document ?? session.Load<Document>(r.DocumentId)));

                    var lazyFacets = documentQuery.AggregateBy(new List<Facet>
                    {
                        new Facet
                            {
                                FieldName = nameof(Person_ForSearch.Result.Age),
                                DisplayFieldName = nameof(Person_ForSearch.Result.Age),
                                Options = new FacetOptions
                                {
                                    TermSortMode = FacetTermSortMode.CountDesc
                                },
                            },
                    }).ExecuteLazy();

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    Assert.NotNull(lazyResults);
                    Assert.Equal(2, lazyResults.Value.Count());
                    Assert.NotNull(lazyFacets);
                    Assert.Single(lazyFacets.Value);
                    Assert.Equal(2, lazyFacets.Value.First().Value.Values.Count);
                }
            }
        }


        public class Person_ForSearch : AbstractIndexCreationTask<Person, Person_ForSearch.Result>
        {
            public class Result : Person
            {
                public string Query { get; set; }
            }
            public Person_ForSearch()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     person.Name,
                                     person.Age,
                                     person.Document,
                                     person.DocumentId,
                                     person.CreatedBy,
                                     person.UpdatedBy,
                                     person.CreateDate,
                                     person.UpdateDate,
                                     Query = AsJson(person).Select(x => x.Value),
                                 };
            }
        }

        public class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public object Document { get; set; }
            public string DocumentId { get; set; }
            public string CreatedBy { get; set; }
            public string UpdatedBy { get; set; }
            public DateTime CreateDate { get; set; }
            public DateTime UpdateDate { get; set; }
        }

        public class Document
        {
            public string Id { get; set; }
            public object Content { get; set; }
            public string CreatedBy { get; set; }
            public string UpdatedBy { get; set; }
            public DateTime CreateDate { get; set; }
            public DateTime UpdateDate { get; set; }
        }
    }

}
