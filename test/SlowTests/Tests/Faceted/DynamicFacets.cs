using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class DynamicFacets : FacetTestBase
    {
        [Fact]
        public void CanPerformDynamicFacetedSearch_Embedded()
        {
            var cameras = GetCameras(30);

            using (var store = GetDocumentStore())
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facets = GetFacets();

                using (var s = store.OpenSession())
                {
                    var expressions = new Expression<Func<Camera, bool>>[]
                    {
                        x => x.Cost >= 100 && x.Cost <= 300,
                        x => x.DateOfListing > new DateTime(2000, 1, 1),
                        x => x.Megapixels > 5.0m && x.Cost < 500,
                        x => x.Manufacturer == "abc&edf"
                    };

                    foreach (var exp in expressions)
                    {
                        var facetResults = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .AggregateBy(facets)
                            .Execute();

                        var filteredData = cameras.Where(exp.Compile()).ToList();

                        CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                    }
                }
            }
        }
        private static readonly List<string> Manufacturers = new List<string>
        {
            "Sony",
            "Nikon",
            "Phillips",
            "Canon",
            "Jessops"
        };
        private static readonly List<string> Models = new List<string>
        {
            "Model1",
            "Model2",
            "Model3",
            "Model4",
            "Model5"
        };
        private static readonly Random Random = new Random(1337);

        public class Foo
        {
            public string Field1 { get; set; }
            public string Field2 { get; set; }
            public string Field3 { get; set; }
            public string Field4 { get; set; }
            public string Field5 { get; set; }
            public string Field6 { get; set; }
            public string Field7 { get; set; }
            public string Field8 { get; set; }
            public string Field9 { get; set; }
            public string Field10 { get; set; }

            private static Random Random => _random;

            private static int _numberOfTerms = 100;
            private static readonly string[] TermsCache = new string[_numberOfTerms];
            private static readonly Random _random = new Random(1337);

            static Foo()
            {
                for (var i = 0; i < _numberOfTerms; i++)
                {
                    TermsCache[i] = "Term" + i;
                }
            }
            public static string GetRandomFooFieldValue()
            {
                return TermsCache[Random.Next(0, _numberOfTerms)];
            }

            public static Foo GenerateFoo()
            {
                return new Foo
                {
                    Field1 = GetRandomFooFieldValue(),
                    Field2 = GetRandomFooFieldValue(),
                    Field3 = GetRandomFooFieldValue(),
                    Field4 = GetRandomFooFieldValue(),
                    Field5 = GetRandomFooFieldValue(),
                    Field6 = GetRandomFooFieldValue(),
                    Field7 = GetRandomFooFieldValue(),
                    Field8 = GetRandomFooFieldValue(),
                    Field9 = GetRandomFooFieldValue(),
                    Field10 = GetRandomFooFieldValue()
                };
            }

            public class FooIndex : AbstractIndexCreationTask<Foo>
            {
                public FooIndex()
                {
                    Map = foos => from foo in foos
                    select new
                    {
                        foo.Field1,
                        foo.Field2,
                        foo.Field3,
                        foo.Field4,
                        foo.Field5,
                        foo.Field6,
                        foo.Field7,
                        foo.Field8,
                        foo.Field9,
                        foo.Field10
                    };
                }
            }
        }
        [Fact]
        public long ProfileFacet()
        {
            using (var store = GetDocumentStore())
            {
                var foos = GenerateFoos(1_000_000);
                using (var bulk = store.BulkInsert())
                {
                    foreach (var foo in foos)
                    {
                        bulk.Store(foo);
                    }
                }
                var fooIndex = new Foo.FooIndex();
                store.ExecuteIndex(fooIndex);
                WaitForIndexing(store, timeout:TimeSpan.FromMinutes(5));
                var facets = new List<FacetBase>
                {
                    new Facet<Foo> {FieldName = x => x.Field1},
                    new Facet<Foo> {FieldName = x => x.Field2},
                    new Facet<Foo> {FieldName = x => x.Field3},
                    new Facet<Foo> {FieldName = x => x.Field4},
                    new Facet<Foo> {FieldName = x => x.Field5},
                    new Facet<Foo> {FieldName = x => x.Field6},
                    new Facet<Foo> {FieldName = x => x.Field7},
                    new Facet<Foo> {FieldName = x => x.Field8},
                    new Facet<Foo> {FieldName = x => x.Field9},
                    new Facet<Foo> {FieldName = x => x.Field10}
                };

                return QueryFoos(store, facets);
            }
        }

        private static long QueryFoos(DocumentStore store, List<FacetBase> facets)
        {
            using (var s = store.OpenSession())
            {
                var sp = Stopwatch.StartNew();
                var facetResults = s.Query<Foo, Foo.FooIndex>()
                    .Where(x =>
                        x.Field1 == Foo.GetRandomFooFieldValue() ||
                        x.Field2 == Foo.GetRandomFooFieldValue() ||
                        x.Field3 == Foo.GetRandomFooFieldValue() ||
                        x.Field4 == Foo.GetRandomFooFieldValue() ||
                        x.Field5 == Foo.GetRandomFooFieldValue() ||
                        x.Field6 == Foo.GetRandomFooFieldValue() ||
                        x.Field7 == Foo.GetRandomFooFieldValue() ||
                        x.Field8 == Foo.GetRandomFooFieldValue() ||
                        x.Field9 == Foo.GetRandomFooFieldValue() ||
                        x.Field10 == Foo.GetRandomFooFieldValue()
                    )
                    .AggregateBy(facets)
                    .Execute();
                return sp.ElapsedMilliseconds;
            }
        }

        private Foo[] GenerateFoos(int amount)
        {
            Foo[] foos = new Foo[amount];
            for (var i = 0; i < amount; i++)
            {
                foos[i] = Foo.GenerateFoo();
            }

            return foos;
        }

        [Fact]
        public void CanPerformDynamicFacetedSearch_Remotely()
        {
            using (var store = GetDocumentStore())
            {
                var cameras = GetCameras(30);

                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facets = GetFacets();

                using (var s = store.OpenSession())
                {
                    var expressions = new Expression<Func<Camera, bool>>[]
                    {
                            x => x.Cost >= 100 && x.Cost <= 300,
                            x => x.DateOfListing > new DateTime(2000, 1, 1),
                            x => x.Megapixels > 5.0m && x.Cost < 500,
                            x => x.Manufacturer == "abc&edf"
                    };

                    foreach (var exp in expressions)
                    {
                        var facetResults = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .AggregateBy(facets)
                            .Execute();

                        var filteredData = cameras.Where(exp.Compile()).ToList();

                        CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                    }
                }
            }
        }

        private void CheckFacetResultsMatchInMemoryData(Dictionary<string, FacetResult> facetResults, List<Camera> filteredData)
        {
            //Make sure we get all range values
            Assert.Equal(filteredData.GroupBy(x => x.Manufacturer).Count(),
                        facetResults["Manufacturer"].Values.Count());

            foreach (var facet in facetResults["Manufacturer"].Values)
            {
                var inMemoryCount = filteredData.Count(x => x.Manufacturer.ToLower() == facet.Range);
                Assert.Equal(inMemoryCount, facet.Count);
            }

            //Go through the expected (in-memory) results and check that there is a corresponding facet result
            //Not the prettiest of code, but it works!!!
            var costFacets = facetResults["Cost"].Values;
            CheckFacetCount(filteredData.Count(x => x.Cost <= 200.0m), costFacets.FirstOrDefault(x => x.Range == "Cost <= 200.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 200.0m && x.Cost <= 400), costFacets.FirstOrDefault(x => x.Range == "Cost between 200.0 and 400.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 400.0m && x.Cost <= 600.0m), costFacets.FirstOrDefault(x => x.Range == "Cost between 400.0 and 600.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 600.0m && x.Cost <= 800.0m), costFacets.FirstOrDefault(x => x.Range == "Cost between 600.0 and 800.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 800.0m), costFacets.FirstOrDefault(x => x.Range == "Cost >= 800.0"));

            //Test the Megapixels_Range facets using the same method
            var megapixelsFacets = facetResults["Megapixels"].Values;
            CheckFacetCount(filteredData.Count(x => x.Megapixels <= 3.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels <= 3.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 3.0 and 7.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 7.0 and 10.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels >= 10.0"));
        }

        private static void CheckFacetCount(int expectedCount, FacetValue facets)
        {
            if (expectedCount > 0)
            {
                Assert.NotNull(facets);
                Assert.Equal(expectedCount, facets.Count);
            }
        }
    }
}
