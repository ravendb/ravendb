using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Wade : RavenTestBase
    {
        public Wade(ITestOutputHelper output) : base(output)
        {
        }

        private class PersonDOBIndex : AbstractIndexCreationTask<Person>
        {
            public PersonDOBIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    BirthDate = person.BirthDate,
                                    Spouse_BirthDate = person.Spouse.BirthDate,
                                    Children_BirthDate = person.Children.Select(z => z.BirthDate)
                                };
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public DateTime BirthDate { get; set; }

            public Person Spouse { get; set; }

            public List<Person> Children { get; set; }

            public Person()
            {
                Children = new List<Person>();
            }
        }


        [Fact]
        public void DateTime_Facet_Works_As_Expected()
        {
            using (var documentStore = GetDocumentStore())
            {

                CreateIndexes(documentStore);

                PopulateDB(documentStore);
                Indexes.WaitForIndexing(documentStore);


                var d1960 = new DateTime(year: 1960, month: 1, day: 1);
                var d1969 = new DateTime(year: 1969, month: 12, day: 31);

                var d1970 = new DateTime(year: 1970, month: 1, day: 1);
                var d1979 = new DateTime(year: 1979, month: 12, day: 31);


                using (var session = documentStore.OpenSession())
                {
                    var employeeFacets = session.Query<Person, PersonDOBIndex>()
                        .Statistics(out var statsEmployeeDOBs)
                        .AggregateBy(
                            builder => builder
                                .ByRanges(
                                    person => person.BirthDate >= d1960 && person.BirthDate < d1969,
                                    person => person.BirthDate >= d1970 && person.BirthDate < d1979
                                )
                                .WithOptions(new FacetOptions { IncludeRemainingTerms = true })
                        )
                        .Execute();

                    // _outputHelper.WriteLine(ToJSONPretty(employeeFacets)); 
                    //output result is expected --
                    //
                    //                    "BirthDate": {
                    //                        "Name": "BirthDate",
                    //                        "Values": [
                    //                        {
                    //                            "Range": "BirthDate >= 1960-01-01T00:00:00.0000000 and BirthDate < 1969-12-31T00:00:00.0000000",
                    //                            "Count": 1,
                    //                            "Sum": null,
                    //                            "Max": null,
                    //                            "Min": null,
                    //                            "Average": null
                    //                        },
                    //                        {
                    //                            "Range": "BirthDate >= 1970-01-01T00:00:00.0000000 and BirthDate < 1979-12-31T00:00:00.0000000",
                    //                            "Count": 0,
                    //                            "Sum": null,
                    //                            "Max": null,
                    //                            "Min": null,
                    //                            "Average": null
                    //                        }
                    //                        ],
                    //                        "RemainingTerms": [],
                    //                        "RemainingTermsCount": 0,
                    //                        "RemainingHits": 0
                    //                    }


                    //employee facets - should have 1 in 60's and 0 in 70's
                    var employee60sCount = employeeFacets.First().Value.Values.First().Count;
                    var employee70sCount = employeeFacets.First().Value.Values.Last().Count;
                    Assert.Equal(employee60sCount, 1);
                    Assert.Equal(employee70sCount, 0);
                }

            }
        }

        [Fact]
        public void Nested_DateTime_Facet_Works_As_Expected()
        {
            using (var documentStore = GetDocumentStore())
            {

                CreateIndexes(documentStore);

                PopulateDB(documentStore);
                Indexes.WaitForIndexing(documentStore);


                var d1960 = new DateTime(year: 1960, month: 1, day: 1);
                var d1969 = new DateTime(year: 1969, month: 12, day: 31);

                var d1970 = new DateTime(year: 1970, month: 1, day: 1);
                var d1979 = new DateTime(year: 1979, month: 12, day: 31);

                //employee facets - should have 0 in 60's and 1 in 70's
                using (var session = documentStore.OpenSession())
                {
                    var spouseFacets = session.Query<Person, PersonDOBIndex>()
                        .Statistics(out var statsSpouseDOBs)
                        .AggregateBy(
                            builder => builder
                                .ByRanges(
                                    person => person.Spouse.BirthDate >= d1960 && person.Spouse.BirthDate < d1969,
                                    person => person.Spouse.BirthDate >= d1970 && person.Spouse.BirthDate < d1979
                                )
                            .WithOptions(new FacetOptions { IncludeRemainingTerms = true })
                        )
                        .Execute();

                    //_outputHelper.WriteLine(ToJSONPretty(spouseFacets));

                    //comments on output below
                    //                    "BirthDate": {                <-- Expected this to be Spouse_BirthDate
                    //                        "Name": "BirthDate",      <-- Expected this to be Spouse_BirthDate
                    //                        "Values": [
                    //                        {
                    //                            "Range": "BirthDate >= 1960-01-01T00:00:00.0000000 and BirthDate < 1969-12-31T00:00:00.0000000",
                    //                            "Count": 1,           <-- Expected this to 0
                    //                            "Sum": null,
                    //                            "Max": null,
                    //                            "Min": null,
                    //                            "Average": null
                    //                        },
                    //                        {
                    //                            "Range": "BirthDate >= 1970-01-01T00:00:00.0000000 and BirthDate < 1979-12-31T00:00:00.0000000",
                    //                            "Count": 0,            <-- Expected this to be 1
                    //                            "Sum": null,
                    //                            "Max": null,
                    //                            "Min": null,
                    //                            "Average": null
                    //                        }
                    //                        ],
                    //                        "RemainingTerms": [],
                    //                        "RemainingTermsCount": 0,
                    //                        "RemainingHits": 0
                    //                    }

                    var spouse60sCount = spouseFacets.First().Value.Values.First().Count;
                    var spouse70sCount = spouseFacets.First().Value.Values.Last().Count;
                    Assert.Equal(spouse60sCount, 0);
                    Assert.Equal(spouse70sCount, 1);

                }

            }
        }

        [Fact]
        public void Nested_Enumberables_DateTime_Facet_Works_As_Expected()
        {
            using (var documentStore = GetDocumentStore())
            {

                CreateIndexes(documentStore);

                PopulateDB(documentStore);
                Indexes.WaitForIndexing(documentStore);

                var d2000 = new DateTime(year: 2000, month: 1, day: 1);
                var d2009 = new DateTime(year: 2009, month: 12, day: 31);

                var d2010 = new DateTime(year: 2010, month: 1, day: 1);
                var d2019 = new DateTime(year: 2019, month: 12, day: 31);

                // employee facets -should have 0 in 60's and 1 in 70's
                using (var session = documentStore.OpenSession())
                {
                    var childrenFacets = session.Query<Person, PersonDOBIndex>()
                        .Statistics(out var statsChildrenDOB)
                        .AggregateBy(
                            builder => builder
                                .ByRanges(
                                    person => person.Children.Any(child => child.BirthDate >= d2000 && child.BirthDate < d2009),
                                    person => person.Children.Any(child => child.BirthDate >= d2010 && child.BirthDate < d2019)
                                )
                                .WithOptions(new FacetOptions { IncludeRemainingTerms = true })
                        )
                        .Execute();

                    //_outputHelper.WriteLine(ToJSONPretty(childrenFacets));

                    var children2000Count = childrenFacets.First().Value.Values.First().Count;
                    var children2010Count = childrenFacets.First().Value.Values.Last().Count;
                    Assert.Equal(children2000Count, 1); //son
                    Assert.Equal(children2010Count, 1); //daughter

                }

            }
        }


        private string ToJSONPretty(object obj)
        {
            return JsonConvert.SerializeObject(obj, Formatting.Indented);
        }

        private static void CreateIndexes(IDocumentStore documentStore)
        {
            new PersonDOBIndex().Execute(documentStore);
        }

        private static void PopulateDB(IDocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                var employee = new Person
                {
                    Id = "employee/1",
                    BirthDate = new DateTime(year: 1969, month: 1, day: 9),
                    Spouse = new Person
                    {
                        BirthDate = new DateTime(year: 1971, month: 11, day: 17)
                    }
                };
                session.Store(employee);


                var son = new Person
                {
                    Id = "son",
                    BirthDate = new DateTime(year: 2007, month: 1, day: 22)
                };
                session.Store(son);
                employee.Children.Add(son);

                var daughter = new Person
                {
                    Id = "daughter",
                    BirthDate = new DateTime(year: 2012, month: 8, day: 7)
                };
                session.Store(daughter);
                employee.Children.Add(daughter);

                session.SaveChanges();
            }
        }
    }
}
