using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Static;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11422 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<User>
        {
            public Index1()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.LastName
                               };

            }
        }

        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public int Age { get; set; }
        }


        private class Users_ByAge : AbstractIndexCreationTask<User, Users_ByAge.Result>
        {
            public class Result
            {
                public int Count { get; set; }
                public int Age { get; set; }
            }

            public Users_ByAge()
            {
                Map = users => from user in users
                               select new
                               {
                                   Age = user.Age,
                                   Count = 1
                               };

                Reduce = results => from result in results
                                    group result by result.Age into g
                                    select new
                                    {
                                        Age = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }


        [Fact]
        public void VerifyIndexScore_Map_SimpleProjection_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry"
                    });
                    session.Store(new User
                    {
                        Name = "Bob"
                    });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from Users select Name"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.Equal(0, indexScore);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_Map_SimpleProjection_AutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia"
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        LastName = "Weir"
                    });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from Users where LastName != null select Name"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_Map_SimpleProjection_StaticIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry"
                    });
                    session.Store(new User
                    {
                        Name = "Bob"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from index 'Index1' select Name"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_MapReduce_SimpleProjection_AutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        Age = 75
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        Age = 67
                    });
                    session.Store(new User
                    {
                        Name = "Bill",
                        Age = 75
                    });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from Users group by Age select key()"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_MapReduce_SimpleProjection_StaticIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByAge().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        Age = 75
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        Age = 67
                    });
                    session.Store(new User
                    {
                        Name = "Bill",
                        Age = 75
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from index 'Users/ByAge' where Count < 10 select Age"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_Map_JsProjection_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia"
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        LastName = "Weir"
                    });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from Users as u select { FullName : u.Name + ' ' + u.LastName }"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.Equal(0, indexScore);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_Map_JsProjection_AutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia"
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        LastName = "Weir"
                    });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from Users as u where u.LastName != null " +
                                "select { Name : u.Name }"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_Map_JsProjection_StaticIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry"
                    });
                    session.Store(new User
                    {
                        Name = "Bob"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from index 'Index1' as i  select { Name : i.Name }"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

        [Fact]
        public void VerifyIndexScore_MapReduce_JsProjection_StaticIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByAge().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        Age = 75
                    });
                    session.Store(new User
                    {
                        Name = "Bob",
                        Age = 67
                    });
                    session.Store(new User
                    {
                        Name = "Bill",
                        Age = 75
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "from index 'Users/ByAge' as i where i.Count < 10 select { Age : i.Age }"
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var results = new DynamicArray(command.Result.Results);
                    Assert.Equal(2, results.Length);

                    foreach (dynamic r in results)
                    {
                        var indexScoreAsString = r[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.IndexScore];
                        Assert.NotNull(indexScoreAsString);
                        var indexScore = float.Parse(indexScoreAsString.ToString(), CultureInfo.InvariantCulture);
                        Assert.True(indexScore > 0);
                    }
                }
            }
        }

    }
}

