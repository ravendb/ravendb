using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Bugs.QueryOptimizer
{
    public class QueryOptimizeTests : RavenNewTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Email { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void WillNotError()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {

                    var blogPosts = from post in session.Query<BlogPost>()
                                    where post.Tags.Any(tag => tag == "RavenDB")
                                    select post;
                    blogPosts.ToList();

                    session.Query<User>()
                        .Where(x => x.Email == "ayende@ayende.com")
                        .ToList();

                    session.Query<User>()
                        .OrderBy(x => x.Name)
                        .ToList();
                }
            }
        }

        [Fact]
        public void CanUseExistingDynamicIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende AND Age:3"
                        });

                    Assert.Equal("Auto/AllDocs/ByAgeAndName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("Auto/AllDocs/ByAgeAndName", queryResult.IndexName);
                }
            }
        }

        [Fact]
        public void CanUseExistingExistingManualIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name, doc.Age }" }
                                                }));

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende AND Age:3"
                        });

                    Assert.Equal("test", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("test", queryResult.IndexName);
                }
            }
        }

        [Fact]
        public void WillCreateWiderIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:3"
                        });

                    Assert.Equal("Auto/AllDocs/ByName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Age:3"
                        });

                    Assert.Equal("Auto/AllDocs/ByAgeAndName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("Auto/AllDocs/ByAgeAndName", queryResult.IndexName);
                }
            }
        }

        [Fact]
        public void WillCreateWiderIndex_UsingEnityName()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic/Users",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:3"
                        });

                    Assert.Equal("Auto/Users/ByName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic/Users",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Age:3"
                        });

                    Assert.Equal("Auto/Users/ByAgeAndName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic/Users",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("Auto/Users/ByAgeAndName", queryResult.IndexName);
                }
            }
        }
        [Fact]
        public void WillCreateWiderIndex_UsingDifferentEntityNames()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic/Users",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:3"
                        });

                    Assert.Equal("Auto/Users/ByName", queryResult.IndexName);

                    queryResult = commands.Query("dynamic/Cars",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Age:3"
                        });

                    Assert.Equal("Auto/Cars/ByAge", queryResult.IndexName);

                    queryResult = commands.Query("dynamic/Users",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("Auto/Users/ByName", queryResult.IndexName);
                }
            }
        }
        [Fact]
        public void WillUseWiderIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name, doc.Age }" }
                                                }));


                store.Admin.Send(new PutIndexOperation("test2",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name }" }
                                                }));

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende AND Age:3"
                        });

                    Assert.Equal("test", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("test", queryResult.IndexName);
                }
            }
        }

        [Fact]
        public void WillAlwaysUseSpecifiedIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name, doc.Age }" }
                                                }));


                store.Admin.Send(new PutIndexOperation("test2",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name }" }
                                                }));

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende AND Age:3"
                        });

                    Assert.Equal("test", queryResult.IndexName);

                    queryResult = commands.Query("test2",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Name:Ayende"
                        });

                    Assert.Equal("test2", queryResult.IndexName);
                }
            }
        }

        [Fact]
        public void WillNotSelectExistingIndexIfFieldAnalyzedSettingsDontMatch()
        {
            //https://groups.google.com/forum/#!topic/ravendb/DYjvNjNIiho/discussion
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Title, doc.BodyText }" },
                                                    Fields = new Dictionary<string, IndexFieldOptions>
                                                    {
                                                        { "Title", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed } }
                                                    }
                                                }));

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "Title:Matt"
                        });

                    //Because the "test" index has a field set to Analyzed (and the default is Non-Analyzed), 
                    //it should NOT be considered a match by the query optimizer!
                    Assert.NotEqual("test", queryResult.IndexName);

                    queryResult = commands.Query("dynamic",
                        new IndexQuery(store.Conventions)
                        {
                            Query = "BodyText:Matt"
                        });
                    //This query CAN use the existing index because "BodyText" is NOT set to analyzed
                    Assert.Equal("test", queryResult.IndexName);
                }
            }
        }

        private class SomeObject
        {
            public string StringField { get; set; }
            public int IntField { get; set; }
        }

        [Fact]
        public void WithRangeQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("SomeObjects/BasicStuff"
                                         , new IndexDefinition
                                         {
                                             Maps = { "from doc in docs.SomeObjects\r\nselect new { IntField = (int)doc.IntField, StringField = doc.StringField }" },
                                             Fields = new Dictionary<string, IndexFieldOptions>
                                             {
                                                 { "IntField", new IndexFieldOptions { Sort = SortOptions.NumericDefault } }
                                             }
                                         }));

                using (IDocumentSession session = store.OpenSession())
                {
                    DateTime startedAt = DateTime.UtcNow;
                    for (int i = 0; i < 40; i++)
                    {
                        var p = new SomeObject
                        {
                            IntField = i,
                            StringField = "user " + i,
                        };
                        session.Store(p);
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var list = session.Query<SomeObject>()
                        .Statistics(out stats)
                        .Where(p => p.StringField == "user 1")
                        .ToList();

                    Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var list = session.Query<SomeObject>()
                        .Statistics(out stats)
                        .Where(p => p.IntField > 150000 && p.IntField < 300000)
                        .ToList();

                    Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var list = session.Query<SomeObject>()
                        .Statistics(out stats)
                        .Where(p => p.StringField == "user 1" && p.IntField > 150000 && p.IntField < 300000)
                        .ToList();

                    Assert.Equal("SomeObjects/BasicStuff", stats.IndexName);
                }
            }
        }
        private class BlogPost
        {
            public string[] Tags { get; set; }
            public string Title { get; set; }
            public string BodyText { get; set; }
        }
    }
}
