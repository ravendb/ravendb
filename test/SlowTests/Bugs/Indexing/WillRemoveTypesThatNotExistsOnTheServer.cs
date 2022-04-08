using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class WillRemoveTypesThatNotExistsOnTheServer : RavenTestBase
    {
        public WillRemoveTypesThatNotExistsOnTheServer(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryAStronglyTypedIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Hibernating Rhinos", Address = new Address { City = "Hadera" } });
                    session.SaveChanges();
                }

                new StronglyTypedIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Result, StronglyTypedIndex>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public Address Address { get; set; }
        }

        private class Address
        {
            public string City { get; set; }
        }

        private class Result
        {
            public string Name { get; set; }
            public Address Address { get; set; }
        }

        private class StronglyTypedIndex : AbstractMultiMapIndexCreationTask<Result>
        {
            public StronglyTypedIndex()
            {
                AddMap<User>(users => users.Select(user => new Result
                {
                    Name = user.Name,
                    Address = user.Address,
                }));

                Reduce = results => from result in results
                                    group result by new { result.Name, result.Address }
                                    into g
                                    select new Result
                                    {
                                        Name = g.Key.Name,
                                        Address = new Address { City = g.Key.Address.City },
                                    };
                
                Stores.Add(i => i.Address, FieldStorage.Yes);
                Index(i => i.Address, FieldIndexing.No);
            }
        }
    }
}
