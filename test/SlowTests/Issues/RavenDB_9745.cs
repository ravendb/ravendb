using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Explanation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9745 : RavenTestBase
    {
        public RavenDB_9745(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Explain()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Micro" });
                    session.Store(new Company { Name = "Microsoft" });
                    session.Store(new Company { Name = "Google" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .IncludeExplanations(out var explanations)
                        .Search(x => x.Name, "Micro*")
                        .ToList();

                    Assert.Equal(2, companies.Count);

                    var exp = explanations.GetExplanations(companies[0].Id);
                    Assert.NotNull(exp);

                    exp = explanations.GetExplanations(companies[1].Id);
                    Assert.NotNull(exp);
                }

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Companies_ByName.Result, Companies_ByName>()
                        .IncludeExplanations(new ExplanationOptions
                        {
                            GroupKey = "Key"
                        }, out var explanations)
                        .ToList();

                    Assert.Equal(3, results.Count);

                    var exp = explanations.GetExplanations(results[0].Key);
                    Assert.NotNull(exp);

                    exp = explanations.GetExplanations(results[1].Key);
                    Assert.NotNull(exp);

                    exp = explanations.GetExplanations(results[2].Key);
                    Assert.NotNull(exp);
                }
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company, Companies_ByName.Result>
        {
            public class Result
            {
                public string Key { get; set; }

                public long Count { get; set; }
            }

            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Key = c.Name,
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.Key into g
                                    select new
                                    {
                                        Key = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                Store(x => x.Key, FieldStorage.Yes);
            }
        }
    }
}
