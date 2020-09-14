using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11089 : FacetTestBase
    {
        public RavenDB_11089(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string NameOfFoo;
            public Bar[] Bars;
        }

        private class Bar
        {
            public float NumberOfBars;
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.NameOfFoo
                              };
            }
        }

        [Fact]
        public void CustomSerializer_WithSaveChanges_AndQuery()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo() { NameOfFoo = "a", Bars = new Bar[] { new Bar() { NumberOfBars = 1.0f }, new Bar() { NumberOfBars = 2.0f } } }, "foo/1");
                    session.Store(new Foo() { NameOfFoo = "b", Bars = new Bar[] { new Bar() { NumberOfBars = 3.0f } } }, "foo/2");
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Foo>()
                        .Customize(x => x.NoCaching())
                        .Customize(x => x.NoTracking())
                        .ToList();

                    Assert.True(q.Count(x => x.NameOfFoo == "a") == 1);
                    Assert.True(q.Count(x => x.NameOfFoo == "b") == 1);
                }
            }
        }

        [Fact]
        public void CustomSerializer_WithSaveChanges_AndLoad()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    var foo1 = new Foo() { NameOfFoo = "a", Bars = new[] { new Bar() { NumberOfBars = 1.0f }, new Bar() { NumberOfBars = 2.0f } } };
                    session.Store(foo1, "foo/1");
                    session.Store(new Foo() { NameOfFoo = "b", Bars = new[] { new Bar() { NumberOfBars = 3.0f } } }, "foo/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var foo1 = session.Load<Foo>("foo/1");
                    Assert.Equal(foo1.NameOfFoo, "a");
                    Assert.Equal(foo1.Bars.Length, 2);
                    Assert.Equal(foo1.Bars[0].NumberOfBars, 1.0f);
                    Assert.Equal(foo1.Bars[1].NumberOfBars, 2.0f);
                }
            }
        }

        [Fact]
        public void CanGetResultsUsingTermVectorsAndStorage()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                string id;

                using (var session = store.OpenSession())
                {
                    new DataIndex(true, true).Execute(store);

                    var list = GetDataList();
                    list.ForEach(session.Store);
                    session.SaveChanges();

                    id = session.Advanced.GetDocumentId(list.First());
                    WaitForIndexing(store);
                }

                AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(store, id);
            }
        }

        [Fact]
        public void CanMakeDynamicDocumentQueriesWithComplexProperties()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                new ComplexDataIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ComplexData
                    {
                        Property = new ComplexProperty
                        {
                            Body = "test"
                        }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<ComplexData, ComplexDataIndex>()
                        .MoreLikeThis(f => f.UsingDocument("{ \"Property\": { \"Body\": \"test\" } }").WithOptions(new MoreLikeThisOptions
                        {
                            MinimumTermFrequency = 1,
                            MinimumDocumentFrequency = 1
                        }));

                    WaitForUserToContinueTheTest(store);

                    var list = query.ToList();

                    Assert.Equal(1, list.Count);
                }
            }
        }

        [Fact]
        public void CanPerformDynamicFacetedSearch_Embedded()
        {
            var cameras = GetCameras(30);

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                new CameraCostIndexStronglyTyped().Execute(store);

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

        [Fact]
        public void CanPerformDynamicFacetedSearch_Remotely()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                var cameras = GetCameras(30);

                new CameraCostIndexStronglyTyped().Execute(store);

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

        private class QueryResult
        {
            public string FullName { get; set; }
        }

        [Fact]
        public void Can_Project_Into_Class()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new QueryResult
                                {
                                    FullName = user.Name + " " + user.LastName
                                };

                    var queryAsString = query.ToString();
                    Assert.Equal("from 'Users' as user select { FullName : user.name+\" \"+user.lastName }", queryAsString);

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void Can_Project_Into_Class_With_Let()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", LastName = "Garcia" }, "users/1");
                    session.Store(new User { Name = "Bob", LastName = "Weir" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let first = user.Name
                                let last = user.LastName
                                let format = (Func<string>)(() => first + " " + last)
                                select new QueryResult
                                {
                                    FullName = format()
                                };

                    var queryAsString = query.ToString();
                    RavenTestHelper.AssertEqualRespectingNewLines(
                    @"declare function output(user) {
	var first = user.name;
	var last = user.lastName;
	var format = function(){return first+"" ""+last;};
	return { FullName : format() };
}
from 'Users' as user select output(user)", queryAsString);

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal("Jerry Garcia", queryResult[0].FullName);
                    Assert.Equal("Bob Weir", queryResult[1].FullName);
                }
            }
        }

        [Fact]
        public void CanPerformIntersectQuery()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                new TShirtIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TShirt
                    {
                        Id = "tshirts/1",
                        Manufacturer = "Raven",
                        ReleaseYear = 2010,
                        Types = new List<TShirtType>
                        {
                            new TShirtType {Color = "Blue", Size = "Small"},
                            new TShirtType {Color = "Black", Size = "Small"},
                            new TShirtType {Color = "Black", Size = "Medium"},
                            new TShirtType {Color = "Gray", Size = "Large"}
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/2",
                        Manufacturer = "Wolf",
                        ReleaseYear = 2011,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Blue",  Size = "Small" },
                            new TShirtType { Color = "Black", Size = "Large" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/3",
                        Manufacturer = "Raven",
                        ReleaseYear = 2011,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Yellow",  Size = "Small" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.Store(new TShirt
                    {
                        Id = "tshirts/4",
                        Manufacturer = "Raven",
                        ReleaseYear = 2012,
                        Types = new List<TShirtType>
                        {
                            new TShirtType { Color = "Blue",  Size = "Small" },
                            new TShirtType { Color = "Gray",  Size = "Large" }
                        }
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var tshirts = session.Query<TShirt, TShirtIndex>()
                        .ProjectInto<TShirtIndex.Result>()
                        .Where(x => x.Manufacturer == "Raven")
                        .Intersect()
                        .Where(x => x.Color == "Blue" && x.Size == "Small")
                        .Intersect()
                        .Where(x => x.Color == "Gray" && x.Size == "Large")
                        .ToArray();

                    Assert.Equal(2, tshirts.Length);
                    Assert.Equal("tshirts/1", tshirts[0].Id);
                    Assert.Equal("tshirts/4", tshirts[1].Id);
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

        private static void AssetMoreLikeThisHasMatchesFor<T, TIndex>(IDocumentStore store, string documentKey)
            where TIndex : AbstractIndexCreationTask, new()
            where T : Identity
        {
            using (var session = store.OpenSession())
            {
                var list = session.Query<T, TIndex>()
                    .MoreLikeThis(f => f.UsingDocument(x => x.Id == documentKey).WithOptions(new MoreLikeThisOptions
                    {
                        Fields = new[] { "Body" }
                    }))
                    .ToList();

                Assert.NotEmpty(list);
            }
        }

        private abstract class Identity
        {
            public string Id { get; set; }
        }

        private class Data : Identity
        {
            public string Body { get; set; }
            public string WhitespaceAnalyzerField { get; set; }
            public string PersonId { get; set; }
        }

        private class ComplexData
        {
            public string Id { get; set; }
            public ComplexProperty Property { get; set; }
        }

        private class ComplexProperty
        {
            public string Body { get; set; }
        }

        private static List<Data> GetDataList()
        {
            var list = new List<Data>
                {
                    new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
                    new Data {Body = "I have a test tomorrow. I hate having a test"},
                    new Data {Body = "Cake is great."},
                    new Data {Body = "This document has the word test only once"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"},
                    new Data {Body = "test"}
                };

            return list;
        }

        private class DataIndex : AbstractIndexCreationTask<Data>
        {
            public DataIndex() : this(true, false)
            {
            }

            public DataIndex(bool termVector, bool store)
            {
                Map = docs => from doc in docs
                              select new { doc.Body, doc.WhitespaceAnalyzerField };

                Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
                {
                    {
                        x => x.Body,
                        typeof (StandardAnalyzer).FullName
                    },
                    {
                        x => x.WhitespaceAnalyzerField,
                        typeof (WhitespaceAnalyzer).FullName
                    }
                };

                if (store)
                {
                    Stores = new Dictionary<Expression<Func<Data, object>>, FieldStorage>
                            {
                                {
                                    x => x.Body, FieldStorage.Yes
                                },
                                {
                                    x => x.WhitespaceAnalyzerField, FieldStorage.Yes
                                }
                            };
                }

                if (termVector)
                {
                    TermVectors = new Dictionary<Expression<Func<Data, object>>, FieldTermVector>
                                {
                                    {
                                        x => x.Body, FieldTermVector.Yes
                                    },
                                    {
                                        x => x.WhitespaceAnalyzerField, FieldTermVector.Yes
                                    }
                                };
                }
            }
        }

        private class ComplexDataIndex : AbstractIndexCreationTask<ComplexData>
        {
            public ComplexDataIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Property,
                                  doc.Property.Body
                              };

                Index(x => x.Property.Body, FieldIndexing.Search);
            }
        }

        [Fact]
        public void PatchOnEnumShouldWork()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var entity = new Job
                    {
                        Title = "Bulk insert",
                        Status = Status.Bad
                    };
                    session.Store(entity);
                    session.SaveChanges();
                    id = session.Advanced.GetDocumentId(entity);
                }

                var expected = Status.Good;
                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Job, Status>(id, x => x.Status, expected);
                    session.SaveChanges();
                    Assert.Equal(expected, session.Load<Job>(id).Status);
                }
            }
        }

        private string _docId = "users/1-A";

        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }

            public Stuff[] Stuff { get; set; }
            public DateTime LastLogin { get; set; }
            public int[] Numbers { get; set; }
        }

        private class Stuff
        {
            public int Key { get; set; }
            public string Phone { get; set; }
            public Pet Pet { get; set; }
            public Friend Friend { get; set; }
            public Dictionary<string, string> Dic { get; set; }
        }

        private class Friend
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public Pet Pet { get; set; }
        }

        private class Pet
        {
            public string Name { get; set; }
            public string Kind { get; set; }
        }

        private string FirstCharToLower(string str) => $"{Char.ToLower(str[0])}{str.Substring(1)}";

        [Fact]
        public void CanPatch()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 31);
                    Assert.Equal(loaded.LastLogin, now);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Stuff[0].Phone, "123456");
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public void CanPatchAndModify()
        {
            var user = new User { Numbers = new[] { 66 } };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    loaded.Numbers[0] = 1;
                    session.Advanced.Patch(loaded, u => u.Numbers[0], 2);
                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        session.SaveChanges();
                    });
                }
            }
        }

        [Fact]
        public void CanPatchComplex()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff[1],
                        new Stuff { Key = 4, Phone = "9255864406", Friend = new Friend() });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Stuff[1].Phone, "9255864406");
                    Assert.Equal(loaded.Stuff[1].Key, 4);
                    Assert.NotNull(loaded.Stuff[1].Friend);

                    session.Advanced.Patch(loaded, u => u.Stuff[2], new Stuff
                    {
                        Key = 4,
                        Phone = "9255864406",
                        Pet = new Pet { Name = "Hanan", Kind = "Dog" },
                        Friend = new Friend
                        {
                            Name = "Gonras",
                            Age = 28,
                            Pet = new Pet { Name = "Miriam", Kind = "Cat" }
                        },
                        Dic = new Dictionary<string, string>
                        {
                            {"Ohio", "Columbus"},
                            {"Utah", "Salt Lake City"},
                            {"Texas", "Austin"},
                            {"California", "Sacramento"},
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Stuff[2].Pet.Name, "Hanan");
                    Assert.Equal(loaded.Stuff[2].Friend.Name, "Gonras");
                    Assert.Equal(loaded.Stuff[2].Friend.Pet.Name, "Miriam");
                    Assert.Equal(loaded.Stuff[2].Dic.Count, 4);
                    Assert.Equal(loaded.Stuff[2].Dic["utah"], "Salt Lake City");
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string AddressId { get; set; }
        }

        private class Index1 : AbstractIndexCreationTask<Person>
        {
            public class Result
            {
                public string CurrentName { get; set; }

                public string PreviousName { get; set; }
            }

            public Index1()
            {
                Map = persons => from person in persons
                                 let metadata = MetadataFor(person)
                                 from name in metadata.Value<string>("Names").Split(',', StringSplitOptions.None)
                                 select new
                                 {
                                     CurrentName = person.Name,
                                     PreviousName = person.Name
                                 };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Person2
        {
            public Person2()
            {
                Family = new Dictionary<string, Person2>();
            }

            public Guid? UserId { get; set; }

            /// <summary>
            ///     Key is CompanyName from DomainConstants.Companies, Value is upline Agent.
            /// </summary>
            public Dictionary<string, Person2> Family { get; set; }

            public string Name { get; set; }

            public string Id
            {
                get { return string.Format("people/{0}", Name); }
            }

            public string IdCopy
            {
                get { return string.Format("people/{0}", Name); }
            }
        }

        private class Person_IdCopy_Index : AbstractIndexCreationTask<Person2>
        {
            public Person_IdCopy_Index()
            {
                Map = people =>
                    from person in people
                    select new
                    {
                        person.Id,
                        StsId = person.UserId,
                        _ = person.Family.Select(x => CreateField("family_" + x.Key + "_Id", x.Value.IdCopy, true, true)),
                    };
            }
        }

        private class PersonIndexItem
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public string Family_Dad_Id { get; set; }
        }

        private class Person_Id_Index : AbstractIndexCreationTask<Person2>
        {
            public Person_Id_Index()
            {
                Map = people =>
                    from person in people
                    select new
                    {
                        person.Id,
                        StsId = person.UserId,
                        _ = person.Family.Select(x => CreateField("Family_" + x.Key + "_Id", x.Value.Id, true, true)),
                    };
            }
        }

        [Fact]
        public void ProjectInto_ShouldWork()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    var person = new Person { Name = "John" };
                    session.Store(person);
                    var metadata = session.Advanced.GetMetadataFor(person);
                    metadata["Names"] = "James,Jonathan";

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<Person, Index1>()
                        .ProjectInto<Index1.Result>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        [Fact]
        public void CanAddToArray()
        {
            var stuff = new Stuff[1];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2 } };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    //push
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(3));
                    session.Advanced.Patch<User, Stuff>(_docId, u => u.Stuff, roles => roles.Add(new Stuff { Key = 75 }));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[2], 3);
                    Assert.Equal(loaded.Stuff[1].Key, 75);

                    //concat
                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(101, 102, 103));
                    session.Advanced.Patch(loaded, u => u.Stuff, roles => roles.Add(new Stuff { Key = 102 }, new Stuff { Phone = "123456" }));
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 6);
                    Assert.Equal(loaded.Numbers[5], 103);

                    Assert.Equal(loaded.Stuff[2].Key, 102);
                    Assert.Equal(loaded.Stuff[3].Phone, "123456");

                    session.Advanced.Patch(loaded, u => u.Numbers, roles => roles.Add(new[] { 201, 202, 203 }));
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers.Length, 9);
                    Assert.Equal(loaded.Numbers[7], 202);
                }
            }
        }

        [Fact]
        public void CanRemoveFromArray()
        {
            var stuff = new Stuff[2];
            stuff[0] = new Stuff { Key = 6 };
            stuff[1] = new Stuff { Phone = "123456" };
            var user = new User { Stuff = stuff, Numbers = new[] { 1, 2, 3 } };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    session.Advanced.Patch<User, object>(_docId, u => u.Stuff, roles => roles.RemoveAt(0));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);

                    Assert.Equal(loaded.Numbers.Length, 2);
                    Assert.Equal(loaded.Numbers[1], 3);

                    Assert.Equal(loaded.Stuff.Length, 1);
                    Assert.Equal(loaded.Stuff[0].Phone, "123456");
                }
            }
        }

        [Fact]
        public void CanIncrement()
        {
            Stuff[] s = new Stuff[3];
            s[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = s };

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Numbers[0], 67);

                    // infer type & the id from entity
                    session.Advanced.Increment(loaded, u => u.Stuff[0].Key, -3);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(loaded.Stuff[0].Key, 3);
                }
            }
        }

        [Fact]
        public void ShouldMergePatchCalls()
        {
            var stuff = new Stuff[3];
            stuff[0] = new Stuff { Key = 6 };
            var user = new User { Numbers = new[] { 66 }, Stuff = stuff };
            var user2 = new User { Numbers = new[] { 1, 2, 3 }, Stuff = stuff };
            var docId2 = "users/2-A";

            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss =>
                {
                    ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer =>
                        {
                            serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                        }
                    };
                    ss.Conventions.PropertyNameConverter = mi => FirstCharToLower(mi.Name);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.Store(user2, docId2);
                    session.SaveChanges();
                }

                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers[0], 31);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(_docId, u => u.LastLogin, now);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(docId2, u => u.Numbers[0], 123);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, DateTime>(docId2, u => u.LastLogin, now);
                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Increment<User, int>(_docId, u => u.Numbers[0], 1);
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(77));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.Add(88));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.Advanced.Patch<User, int>(_docId, u => u.Numbers, roles => roles.RemoveAt(1));
                    Assert.Equal(1, ((InMemoryDocumentSessionOperations)session).DeferredCommandsCount);

                    session.SaveChanges();
                }
            }
        }

        public class Job
        {
            public string Title { get; set; }
            public Status Status { get; set; }
        }

        public enum Status
        {
            None,
            Good,
            Bad
        }
    }
}
