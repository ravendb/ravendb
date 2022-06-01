using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_13434 : RavenTestBase
    {
        public RavenDB_13434(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData]
        public void CanUseSplitOptionInSearchQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new GeekPerson
                    {
                        Name = "One",
                        FavoritePrimes = new[] { 1 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "Seven",
                        FavoritePrimes = new[] { 7 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "OneTwo",
                        FavoritePrimes = new[] { 1, 2 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "OneTwoThree",
                        FavoritePrimes = new[] { 1, 2, 3 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "OneTwoThreeFive",
                        FavoritePrimes = new[] { 1, 2, 3, 5 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "TwoFive",
                        FavoritePrimes = new[] { 2, 5 },
                    });

                    session.Store(new GeekPerson
                    {
                        Name = "TwoFiveSeven",
                        FavoritePrimes = new[] { 2, 5, 7 },
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // testing "and" terms search
                    var res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "1 2 3 5", @operator: Raven.Client.Documents.Queries.SearchOperator.And).ToList();
                    Assert.Equal(1, res.Count);
                    Assert.Equal("OneTwoThreeFive", res.First().Name);

                    // testing "and" terms search, partial values list
                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "1 3 5", @operator: Raven.Client.Documents.Queries.SearchOperator.And).ToList();
                    Assert.Equal(1, res.Count);
                    Assert.Equal("OneTwoThreeFive", res.First().Name);

                    // testing "and" terms search, no results expected
                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "1 2 3 5 7", @operator: Raven.Client.Documents.Queries.SearchOperator.And).ToList();
                    Assert.Equal(0, res.Count);

                    // testing "or" search, where one of the values is irrelevant
                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "2 9", @operator: Raven.Client.Documents.Queries.SearchOperator.Or).ToList();
                    Assert.Equal(5, res.Count);
                    Assert.Contains("OneTwo", res.Select(x => x.Name));
                    Assert.Contains("OneTwoThree", res.Select(x => x.Name));
                    Assert.Contains("OneTwoThreeFive", res.Select(x => x.Name));
                    Assert.Contains("TwoFive", res.Select(x => x.Name));
                    Assert.Contains("TwoFiveSeven", res.Select(x => x.Name));


                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "8 9 10", options: SearchOptions.Not, @operator: Raven.Client.Documents.Queries.SearchOperator.Or).ToList();
                    Assert.Equal(7, res.Count);

                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "1 2 3 5 7", options: SearchOptions.Not, @operator: Raven.Client.Documents.Queries.SearchOperator.Or).ToList();
                    Assert.Equal(0, res.Count);

                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "1 2 3 5 7", options: SearchOptions.Not, @operator: Raven.Client.Documents.Queries.SearchOperator.And).ToList();
                    Assert.Equal(7, res.Count);

                    res = session.Query<GeekPerson>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FavoritePrimes, "2 5", options: SearchOptions.Not, @operator: Raven.Client.Documents.Queries.SearchOperator.And).ToList();
                    Assert.Equal(4, res.Count);



                }
            }
        }
    }
}
