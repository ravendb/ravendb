using System.Linq;
using FastTests;
using Raven.Server.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Queries.Dynamic
{
    public class RavenDB_8806 : RavenTestBase
    {
        public RavenDB_8806(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_stream_dynamic_query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe" });
                    session.Store(new User { Name = "Dave" });
                    session.Store(new User { Name = "Joe" });

                    session.SaveChanges();
                }

                var count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>(); // dynamic collection query

                    using (var reader = session.Advanced.Stream(query))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }

                Assert.Equal(3, count);

                count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>().Search(x => x.Name, "j*");

                    using (var reader = session.Advanced.Stream(query))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }

                Assert.Equal(2, count);

                count = 0;

                //Sharding is not supporting Map-Reduce streaming queries.
                if (options.DatabaseMode is RavenDatabaseMode.Single)
                {
                    using (var session = store.OpenSession())
                    {
                        var query = session.Query<User>().GroupBy(x => x.Name).Select(x => new Result {Name = x.Key, Count = x.Count()});

                        using (var reader = session.Advanced.Stream(query))
                        {
                            while (reader.MoveNext())
                            {
                                count++;
                                Assert.IsType<Result>(reader.Current.Document);
                            }
                        }
                    }

                    Assert.Equal(2, count);
                }
            }
        }

        private class Result
        {
            public string Name { get; set; }

            public int Count { get; set; }
        }
    }
}
