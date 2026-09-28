using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13497 : RavenTestBase
    {
        public RavenDB_13497(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        public void ThrowsWhenReduceRenamesMapField()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersReducedWithRenamedField()));

                Assert.Contains("must return all fields of its Map functions in the Reduce function", e.Message);
                Assert.Contains("Missing fields: Count", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        public void ThrowsWhenReduceReturnsSubsetOfMapFields()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersReducedWithMissingField()));

                Assert.Contains("must return all fields of its Map functions in the Reduce function", e.Message);
                Assert.Contains("Missing fields: Count", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        public void ThrowsWhenMapsOfMultiMapIndexReturnDifferentFields()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersAndEmployeesReducedWithNonMatchingMaps()));

                Assert.Contains("must return identical fields in all its Map functions", e.Message);
            }
        }

        [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AllowsReduceWithAdditionalDerivedFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new UsersReducedWithDerivedField());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe", Age = 33 });
                    session.Store(new User { Name = "Joe", Age = 34 });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<ReduceResults>("UsersReducedWithDerivedField").ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Joe", results[0].Name);
                    Assert.Equal(2, results[0].Count);
                    Assert.Equal("Multiple", results[0].Status);
                }
            }
        }

        [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        public void ThrowsWhenGroupByFieldIsMissingFromMap()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersReducedByFieldMissingFromMap()));

                Assert.Contains("is grouping by field 'Age' which is not returned by its map functions", e.Message);
            }
        }

        [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanCreateAndUseMapReduceIndexWithMatchingFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new UsersReducedByName());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe", Age = 33 });
                    session.Store(new User { Name = "Joe", Age = 34 });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<ReduceResults>("UsersReducedByName").ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Joe", results[0].Name);
                    Assert.Equal(2, results[0].Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SkipsValidationWhenMapHasDynamicReturns(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new UsersReducedByNameResultsPushedToJsArray());

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe", Age = 33 });
                    session.Store(new User { Name = "Joe", Age = 34 });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<ReduceResults>("UsersReducedByNameResultsPushedToJsArray").ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Joe", results[0].Name);
                    Assert.Equal(2, results[0].Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
        public void CompilationSkipsValidationSoExistingIndexesKeepLoading()
        {
            var definition = new IndexDefinition
            {
                Name = "UsersReducedWithRenamedField",
                Maps = new HashSet<string> { Maps.MatchingMap },
                Reduce = Maps.ReduceWithRenamedField
            };

            var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
            configuration.Initialize();

            var index = new JavaScriptIndex(definition, configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
            Assert.NotNull(index.ReduceOperation);

            Assert.Throws<IndexCreationException>(() => index.ValidateFieldsOfMapAndReduceFunctions());
        }

        private class ReduceResults
        {
            public string Name { get; set; }

            public int Count { get; set; }

            public string Status { get; set; }
        }

        private static class Maps
        {
            public const string MatchingMap = "map('Users', function (u) { return { Name: u.Name, Count: 1 }; })";

            public const string MatchingReduce = @"groupBy(x => x.Name)
                .aggregate(g => { return { Name: g.key, Count: g.values.reduce((total, val) => val.Count + total, 0) }; })";

            public const string ReduceWithRenamedField = @"groupBy(x => x.Name)
                .aggregate(g => { return { Name: g.key, Total: g.values.reduce((total, val) => val.Count + total, 0) }; })";
        }

        private class UsersReducedWithRenamedField : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedWithRenamedField()
            {
                Maps = new HashSet<string> { RavenDB_13497.Maps.MatchingMap };
                Reduce = RavenDB_13497.Maps.ReduceWithRenamedField;
            }
        }

        private class UsersReducedWithDerivedField : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedWithDerivedField()
            {
                Maps = new HashSet<string> { RavenDB_13497.Maps.MatchingMap };
                Reduce = @"groupBy(x => x.Name)
                    .aggregate(g => {
                        var count = g.values.reduce((total, val) => val.Count + total, 0);
                        return { Name: g.key, Count: count, Status: count > 1 ? 'Multiple' : 'Single' };
                    })";
            }
        }

        private class UsersReducedWithMissingField : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedWithMissingField()
            {
                Maps = new HashSet<string> { RavenDB_13497.Maps.MatchingMap };
                Reduce = @"groupBy(x => x.Name)
                    .aggregate(g => { return { Name: g.key }; })";
            }
        }

        private class UsersAndEmployeesReducedWithNonMatchingMaps : AbstractJavaScriptIndexCreationTask
        {
            public UsersAndEmployeesReducedWithNonMatchingMaps()
            {
                Maps = new HashSet<string>
                {
                    RavenDB_13497.Maps.MatchingMap,
                    "map('Employees', function (e) { return { Name: e.FirstName, Total: 1 }; })"
                };
                Reduce = RavenDB_13497.Maps.MatchingReduce;
            }
        }

        private class UsersReducedByFieldMissingFromMap : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedByFieldMissingFromMap()
            {
                Maps = new HashSet<string> { RavenDB_13497.Maps.MatchingMap };
                Reduce = @"groupBy(x => x.Age)
                    .aggregate(g => { return { Name: g.key, Count: g.values.reduce((total, val) => val.Count + total, 0) }; })";
            }
        }

        private class UsersReducedByName : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedByName()
            {
                Maps = new HashSet<string> { RavenDB_13497.Maps.MatchingMap };
                Reduce = RavenDB_13497.Maps.MatchingReduce;
            }
        }

        private class UsersReducedByNameResultsPushedToJsArray : AbstractJavaScriptIndexCreationTask
        {
            public UsersReducedByNameResultsPushedToJsArray()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (u) {
                        var res = [];
                        res.push({ Name: u.Name, Count: 1 });
                        return res;
                    })"
                };
                Reduce = RavenDB_13497.Maps.MatchingReduce;
            }
        }
    }
}
