using System;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing
{
    public class RavenDB_8257 : RavenTestBase
    {
        public RavenDB_8257(ITestOutputHelper output) : base(output)
        {
        }

        public class ReduceIndex : AbstractIndexCreationTask<User>
        {
            public ReduceIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Age,
                                   user.Name,
                                   Count = 1
                               };
                Reduce = results => from result in results
                                    group result by new { result.Age, result.Name } into g
                                    select new
                                    {
                                        Age = g.Key.Age,
                                        Name = g.Key.Name,
                                        Count = g.Sum(x => x.Count)
                                    };

                Stores.Add(x => x.Age, FieldStorage.Yes);
                Stores.Add(x => x.Count, FieldStorage.Yes);
            }
        }
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ReduceIndexProjectionWithoutStoredFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new ReduceIndex().Execute(store);

                CreateUsers(store);

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced
                        .RawQuery<dynamic>(@"from index 'ReduceIndex' as user
where user.Name = 'Vasiliy' and user.Age = 20
select {
Name: user.Name,
Count: user.Count
}")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();
                    Assert.NotEmpty(products);
                    Assert.Equal("Vasiliy", products.First().Name.ToString());
                    Assert.Equal("2", products.First().Count.ToString());
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ReduceIndexProjectionWithStoredFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new ReduceIndex().Execute(store);

                CreateUsers(store);

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.RawQuery<dynamic>(@"from index 'ReduceIndex' as user
where user.Name = 'Vasiliy' and user.Age = 20
select {
Age: user.Age,
Count: user.Count
}")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();
                    Assert.NotEmpty(products);
                    Assert.Equal("20", products.First().Age.ToString());
                    Assert.Equal("2", products.First().Count.ToString());
                }
            }
        }


        private static void CreateUsers(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    Age = 20,
                    Name = "Vasiliy",
                    LastName = "Petkin"
                });

                session.Store(new User
                {
                    Age = 20,
                    Name = "Vasiliy",
                    LastName = "Voronov"
                });

                session.Store(new User
                {
                    Age = 20,
                    Name = "John",
                    LastName = "Crow"
                });

                session.Store(new User
                {
                    Age = 25,
                    Name = "Mary",
                    LastName = "Antoinette"
                });

                session.SaveChanges();
            }
        }
    }
}
