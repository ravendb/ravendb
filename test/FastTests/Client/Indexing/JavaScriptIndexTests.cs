using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class JavaScriptIndexTests : RavenTestBase
    {
        [Fact]
        public void CreatingJavaScriptIndexWithFeaturesAvailabilitySetToStableWillThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = null; // by default we should have Stable features
                }
            }))
            {
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersByName()));
                Assert.Contains(
                    "Database does not support 'JavaScript' indexes. Please enable experimental features by changing 'Features.Availability' configuration value to 'Experimental'.",
                    e.Message);
            }
        }

        [Fact]
        public void CanUseJavaScriptIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersByName").Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanUseJavaScriptIndexWithDynamicFields()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByNameAndAnalyzedName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitForUserToContinueTheTest(store);
                    session.Query<User>("UsersByNameAndAnalyzedName").ProjectInto<UsersByNameAndAnalyzedName.Result>().Search(x => x.AnalyzedName, "Brendan")
                        .Single();
                }

            }
        }

        [Fact]
        public void CanUseJavaScriptMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersAndProductsByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.Store(new Product
                    {
                        Name = "Shampoo",
                        IsAvailable = true
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersAndProductsByName").Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanUseJavaScriptIndexWithLoadDocument()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersWithProductsByName());
                using (var session = store.OpenSession())
                {
                    var productId = "Products/1";
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true,
                        Product = productId
                    });
                    session.Store(new Product
                    {
                        Name = "Shampoo",
                        IsAvailable = true
                    }, productId);
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersWithProductsByName").Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanElivateSimpleFunctions()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByNameAndIsActive());
                using (var session = store.OpenSession())
                {

                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersByNameAndIsActive").Single(x => x.Name == "Brendan Eich" && x.IsActive == true);
                }

            }
        }
        [Fact]
        public void CanUseJavaScriptMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersAndProductsByNameAndCount());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.Store(new Product
                    {
                        Name = "Shampoo",
                        IsAvailable = true
                    });
                    session.SaveChanges();
                    WaitForUserToContinueTheTest(store);
                    WaitForIndexing(store);
                    session.Query<User>("UsersAndProductsByNameAndCount").OfType<ReduceResults>().Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        private class User
        {
            public string Name { get; set; }
            public bool IsActive { get; set; }
            public string Product { get; set; }
        }

        private class Product
        {
            public string Name { get; set; }
            public bool IsAvailable { get; set; }
        }

        private class ReduceResults
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByName",
                    Maps = new HashSet<string>
                    {
                        @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersByNameAndAnalyzedName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByName",
                    Maps = new HashSet<string>
                    {
                        @"
map('Users', function (u){ 
    return { 
        Name: u.Name, 
        _: {$value: u.Name, $name:'AnalyzedName', $options:{index: true, store: true}}
    };
})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Search,
                                Analyzer = "StandardAnalyzer"
                            }
                        }
                    },
                    Configuration = new IndexConfiguration()
                };

            }

            public class Result
            {
                public string AnalyzedName { get; set; }
            }
        }
        
        private class UsersAndProductsByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersAndProductsByName",
                    Maps = new HashSet<string>
                        {
                            @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
                            @"map('Products', function (p){ return { Name: p.Name, Count: 1};})"
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersByNameAndIsActive : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByNameAndIsActive",
                    Maps = new HashSet<string>
                    {
                        @"map('Users', u => u.Name, function(f){ return f.IsActive;})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersWithProductsByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersWithProductsByName",
                    Maps = new HashSet<string>
                        {
                            @"map('Users', function (u){ return { Name: u.Name, Count: 1, Product: load(u.Product,'Products').Name};})",
                        },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class UsersAndProductsByNameAndCount : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersAndProductsByNameAndCount",
                    Maps = new HashSet<string>
                        {
                            @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
                            @"map('Products', function (p){ return { Name: p.Name, Count: 1};})"
                        },
                    Reduce = @"groupBy( x =>  x.Name )
                                .aggregate(g => {return {
                                    Name: g.key,
                                    Count: g.values.reduce((total, val) => val.Count + total,0)
                               };})",
                    Type = IndexType.JavaScriptMapReduce,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
    }
}
