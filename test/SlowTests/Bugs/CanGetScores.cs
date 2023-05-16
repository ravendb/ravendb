using FastTests;
using System.Linq;
using Lucene.Net.Analysis;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs
{
    public class CanGetScores : RavenTestBase
    {
        public CanGetScores(ITestOutputHelper output) : base(output)
        {
        }

        private IndexFieldOptions filedOptions = new IndexFieldOptions { Indexing = FieldIndexing.Search };

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void FromQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "who is knocking on my doors" });
                    s.Store(new User { Name = "doors ltd" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name}"},
                    Fields = { { "Name", filedOptions } },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var users = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "doors")
                        .ToList();
                    Assert.Equal(2, users.Count);
                    foreach (var user in users)
                    {
                        var score = s.Advanced.GetMetadataFor(user)["@index-score"];
                        Assert.NotNull(score);
                    }
                }
            }
        }


        [Theory]
        [RavenData]
        public void FromQueryWithOrderByScoreThenName(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "who is knocking on my doors" });
                    s.Store(new User { Name = "doors doors ltd" });
                    s.Store(new User { Name = "doors doors abc" });
                    s.SaveChanges();
                }

                // Overloading the email property into a catchall freeform container to avoid rewriting the test entirely.

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Name, Name = doc.Name }" },
                    Fields = { { "Email", filedOptions } },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var users = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "doors")
                        .OrderByScore().ThenBy(x => x.Name)
                        .ToList();

                    Assert.Equal(3, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby score descending, u.Name
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i]).GetDouble(Constants.Documents.Metadata.IndexScore);
                        Assert.True(score > 0.1);
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void FromQueryWithOrderByScoreThenNameDescending(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "who is knocking on my doors" });
                    s.Store(new User { Name = "doors doors ltd" });
                    s.Store(new User { Name = "doors doors abc" });
                    s.SaveChanges();
                }

                // Overloading the email property into a catchall freeform container to avoid rewriting the test entirely.

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Name, Name = doc.Name }" },
                    Fields = { { "Email", filedOptions } },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var users = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "doors")
                        .OrderByScore().ThenByDescending(x => x.Name)
                        .ToList();

                    Assert.Equal(3, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby score descending, u.Name descending
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i])["@index-score"];
                        Assert.NotNull(score);
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void FromQueryWithOrderByNameThenByScore(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "b", Email = "aviv@ravendb.net" });
                    s.Store(new User { Name = "b", Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "a", Email = "aviv@gmail.com" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Email, Name = doc.Name }" },
                    Fields =
                    {
                        { "Email", new IndexFieldOptions
                            {
                                Indexing = FieldIndexing.Search,
                                Analyzer = typeof(SimpleAnalyzer).FullName
                            }
                        }
                    },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "aviv")
                        .OrderBy(x => x.Name)
                        .ThenByScore();

                    Assert.Equal("from index 'test' where Email = $p0 order by Name, score()"
                        , query.ToString());

                    var users = query.ToList();

                    Assert.Equal(3, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby u.Name, score descending
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i]).GetDouble(Constants.Documents.Metadata.IndexScore);
                        Assert.True(score > 0.1);
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void FromQueryWithOrderByNameThenByScoreDescending(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "b", Email = "aviv@ravendb.net" });
                    s.Store(new User { Name = "b", Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "a", Email = "aviv@gmail.com" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Email, Name = doc.Name }" },
                    Fields =
                    {
                        { "Email", new IndexFieldOptions
                            {
                                Indexing = FieldIndexing.Search,
                                Analyzer = typeof(SimpleAnalyzer).FullName
                            }
                        }
                    },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "aviv")
                        .OrderBy(x => x.Name)
                        .ThenByScoreDescending();

                    Assert.Equal("from index 'test' where Email = $p0 order by Name, score() desc"
                        , query.ToString());

                    var users = query.ToList();

                    Assert.Equal(3, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby u.Name, score
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i]).GetDouble(Constants.Documents.Metadata.IndexScore);
                        Assert.True(score > 0.1);
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void FromQueryWithOrderByNameThenByScoreThenByAge(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "b", Age = 32, Email = "aviv@ravendb.net" });
                    s.Store(new User { Name = "b", Age = 33, Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "b", Age = 32, Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "a", Age = 32, Email = "aviv@gmail.com" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Email, Name = doc.Name, Age = doc.Age }" },
                    Fields =
                    {
                        { "Email", new IndexFieldOptions
                            {
                                Indexing = FieldIndexing.Search,
                                Analyzer = typeof(SimpleAnalyzer).FullName
                            }
                        }
                    },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "aviv")
                        .OrderBy(x => x.Name)
                        .ThenByScore()
                        .ThenBy(x => x.Age);

                    Assert.Equal("from index 'test' where Email = $p0 order by Name, score(), Age as long"
                        , query.ToString());

                    var users = query.ToList();

                    Assert.Equal(4, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby u.Name, score descending, u.Age
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i]).GetDouble(Constants.Documents.Metadata.IndexScore);
                        Assert.True(score > 0.1);
                    }
                }
            }
        }

        [Theory]
        [RavenData]
        public void FromQueryWithOrderByNameThenByScoreDescendingThenByAge(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "b", Age = 32, Email = "aviv@ravendb.net" });
                    s.Store(new User { Name = "b", Age = 33, Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "b", Age = 32, Email = "aviv@aviv.com" });
                    s.Store(new User { Name = "a", Age = 32, Email = "aviv@gmail.com" });
                    s.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { Email = doc.Email, Name = doc.Name, Age = doc.Age }" },
                    Fields =
                    {
                        { "Email", new IndexFieldOptions
                            {
                                Indexing = FieldIndexing.Search,
                                Analyzer = typeof(SimpleAnalyzer).FullName
                            }
                        }
                    },
                    Name = "test"
                }}));

                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>("test")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Email == "aviv")
                        .OrderBy(x => x.Name)
                        .ThenByScoreDescending()
                        .ThenBy(x => x.Age);

                    Assert.Equal("from index 'test' where Email = $p0 order by Name, score() desc, Age as long"
                        , query.ToString());

                    var users = query.ToList();

                    Assert.Equal(4, users.Count);

                    var sorted = (from u in users
                                  let score = s.Advanced.GetMetadataFor(u).GetDouble(Constants.Documents.Metadata.IndexScore)
                                  orderby u.Name, score, u.Age
                                  select new { score, u.Name }).ToList();

                    for (var i = 0; i < users.Count; i++)
                    {
                        Assert.Equal(sorted[i].Name, users[i].Name);
                        var score = s.Advanced.GetMetadataFor(users[i]).GetDouble(Constants.Documents.Metadata.IndexScore);
                        Assert.True(score > 0.1);
                    }
                }
            }
        }


        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

    }
}
