using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class JavaScriptIndexTests : RavenTestBase
    {
        [Fact]
        public void CreatingJavaScriptIndexWithFeaturesAvailabilitySetToStableWillThrow()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                Server.Configuration.Core.FeaturesAvailability = FeaturesAvailability.Stable;
                var e = Assert.Throws<IndexCreationException>(() => store.ExecuteIndex(new UsersByName()));
                Assert.Contains(
                    "The experimental 'Javascript' indexes feature is not enabled in your current server configuration. " +
                    "In order to use, please enable experimental features by changing 'Features.Availability' configuration value to 'Experimental'.",
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
        public void CanIndexTimeSpan()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TeemoByDuration());
                using (var session = store.OpenSession())
                {
                    session.Store(new Teemo
                    {
                        Description = "5 minutes",
                        Duration = TimeSpan.FromMinutes(5)
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<Teemo>("TeemoByDuration").Single(x => x.Duration == TimeSpan.FromMinutes(5));
                }

            }
        }

        public class Teemo
        {
            public string Description { get; set; }
            public TimeSpan Duration { get; set; }
        }

        [Fact]
        public void CanUseJavaScriptIndexWithAdditionalSources()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByNameWithAdditionalSources());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Brendan Eich",
                        IsActive = true
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User>("UsersByNameWithAdditionalSources").Single(x => x.Name == "Mr. Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanIndexArrayProperties()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersByPhones());
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jow",
                        PhoneNumbers = new [] {"555-234-8765","555-987-3425"}
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var result = session.Query<UsersByPhones.UsersByPhonesResult>("UsersByPhones")
                        .Where(x => x.Phone == "555-234-8765")
                        .OfType<User>()
                        .Single();
                }

            }
        }

        private class Fanout
        {
            public string Foo { get; set; }
            public int[] Numbers { get; set; }
        }

        [Fact]
        public void CanIndexMapWithFanout()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new FanoutByNumbers());
                using (var session = store.OpenSession())
                {
                    session.Store(new Fanout
                    {
                        Foo = "Foo",
                        Numbers = new[] {4,6,11,9 }
                    });
                    session.Store(new Fanout
                    {
                        Foo = "Bar",
                        Numbers = new[] { 3, 8, 5, 17 }
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var result = session.Query<FanoutByNumbers.Result>("FanoutByNumbers")
                        .Where(x => x.Sum == 17 )
                        .OfType<Fanout>()
                        .Single();
                    Assert.Equal("Bar", result.Foo);
                }

            }
        }

        [Fact]
        public void CanIndexMapReduceWithFanout()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new FanoutByNumbersWithReduce());
                using (var session = store.OpenSession())
                {
                    session.Store(new Fanout
                    {
                        Foo = "Foo",
                        Numbers = new[] { 4, 6, 11, 9 }
                    });
                    session.Store(new Fanout
                    {
                        Foo = "Bar",
                        Numbers = new[] { 3, 8, 5, 17 }
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitForUserToContinueTheTest(store);
                    var result = session.Query<FanoutByNumbersWithReduce.Result>("FanoutByNumbersWithReduce")
                        .Where(x => x.Sum == 33)
                        .Single();
                    Assert.Equal("Bar", result.Foo);
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
                    WaitForIndexing(store);
                    session.Query<User>("UsersAndProductsByNameAndCount").OfType<ReduceResults>().Single(x => x.Name == "Brendan Eich");
                }

            }
        }

        [Fact]
        public void CanUseSpatialFields()
        {
            var kalab = 10;
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Spatial());
                CanUseSpatialFieldsInternal(kalab, store, "Spatial");
            }
        }

        [Fact]
        public void CanUseDynamicSpatialFields()
        {
            var kalab = 10;
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new DynamicSpatial());
                CanUseSpatialFieldsInternal(kalab, store, "DynamicSpatial");
            }
        }

        private static void CanUseSpatialFieldsInternal(int kalab, DocumentStore store, string indexName)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Location
                {
                    Description = "Dor beach",
                    Latitude = 32.61059534196809,
                    Longitude = 34.918146686510454

                });
                session.Store(new Location
                {
                    Description = "Kfar Galim",
                    Latitude = 32.76724701152615,
                    Longitude = 34.957999421620116

                });
                session.SaveChanges();
                WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                session.Query<Location>(indexName).Spatial("Location", criteria => criteria.WithinRadius(kalab, 32.56829122491778, 34.953954053921734)).Single(x => x.Description == "Dor beach");
            }
        }

        [Fact]
        public void CanReduceNullValues()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new UsersReducedByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = null});
                    session.Store(new User { Name = null });
                    session.Store(new User { Name = null });
                    session.Store(new User { Name = "Tal" });
                    session.Store(new User { Name = "Maxim" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var res = session.Query<User>("UsersReducedByName").OfType<ReduceResults>().Single(x => x.Count == 3);
                    Assert.Null(res.Name);
                }

            }
        }
        private class User
        {
            public string Name { get; set; }
            public bool IsActive { get; set; }
            public string Product { get; set; }
            public string[] PhoneNumbers { get; set; }
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

        private class TeemoByDuration : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "TeemoByDuration",
                    Maps = new HashSet<string>
                    {
                        @"map('Teemos', function (t){ return { Duration: t.Duration, Description: t.Description};})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }
        private class UsersByNameWithAdditionalSources : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByName",
                    Maps = new HashSet<string>
                    {
                        @"map('Users', function (u){ return { Name: Mr(u.Name)};})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration(),
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["The Script"] = @"
function Mr(x){
    return 'Mr. ' + x;
}"
                    }
                    
                };
            }
        }
        private class FanoutByNumbers : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "FanoutByNumbers",
                    Maps = new HashSet<string>
                    {
                        @"map('Fanouts', function (f){ 
 var result = [];
for(var i = 0; i < f.Numbers.length; i++)
{
    result.push({
        Foo: f.Foo,
        Sum: f.Numbers[i]
    });
}
return result;
})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }

            internal class Result
            {
                public string Foo { get; set; }
                public int Sum { get; set; } 
            }
        }

        private class FanoutByNumbersWithReduce : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "FanoutByNumbersWithReduce",
                    Maps = new HashSet<string>
                    {
                        @"map('Fanouts', function (f){ 
 var result = [];
for(var i = 0; i < f.Numbers.length; i++)
{
    result.push({
        Foo: f.Foo,
        Sum: f.Numbers[i]
    });
}
return result;
})",
                    },
                    Reduce =
                    @"
groupBy( f => f.Foo )
 .aggregate( g => ({
     Foo: g.key,
     Sum: g.values.reduce((total, val) => val.Sum + total,0)
 }))",
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }

            internal class Result
            {
                public string Foo { get; set; }
                public int Sum { get; set; }
            }
        }

        private class UsersByPhones : AbstractIndexCreationTask
        {
            public class UsersByPhonesResult
            {
                public string Name { get; set; }
                public string Phone { get; set; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersByPhones",
                    Maps = new HashSet<string>
                    {
                        @"map('Users', function (u){ return { Name: u.Name, Phone: u.PhoneNumbers};})",
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

        private class Location
        {
            public string Description { get; set; }
            public double Longitude { get; set; }
            public double Latitude { get; set; }
        }

        private class Spatial : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Spatial",
                    Maps = new HashSet<string>
                    {
                        @"map('Locations', function (l){ return { Description: l.Description, Location: createSpatialField(l.Latitude, l.Longitude)}})",
                    },
                    Type = IndexType.JavaScriptMap,
                    LockMode = IndexLockMode.Unlock,
                    Priority = IndexPriority.Normal,
                    Configuration = new IndexConfiguration()
                };
            }
        }

        private class DynamicSpatial : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Spatial",
                    Maps = new HashSet<string>
                    {
                        @"map('Locations', function (l){ return { Description: l.Description, _:{$value: createSpatialField(l.Latitude, l.Longitude), $name:'Location', $options:{index: true, store: true}} }})",
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

        private class UsersReducedByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "UsersReducedByName",
                    Maps = new HashSet<string>
                    {
                        @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
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
