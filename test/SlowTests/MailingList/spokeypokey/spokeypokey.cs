using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList.spokeypokey
{
    public class spokeypokey : RavenTestBase
    {
        private class BarnIndex : AbstractIndexCreationTask<Barn, Barn>
        {
            public BarnIndex()
            {
                Map =
                    barnlist =>
                    from barn in barnlist
                    from household in barn.Households
                    from member in household.Members
                    select new
                    {
                        barn.InternalId,
                        barn.Name,
                        HouseholdId = household.InternalId,
                        MemberId = member.InternalId,
                        MembersName = member.Name
                    };

            }
        }

        [Fact]
        public void Can_use_barn_index2()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.FindPropertyNameForIndex = (indexedType, indexedName, path, prop) =>
                    {
                        var result = path + prop;
                        switch (result)
                        {
                            case "Households[].Members[].Name":
                                return "MembersName";
                            default:
                                return result;
                        }
                    };
                }
            }))
            {
                new BarnIndex().Execute(store);

                var barn1 = new Barn
                {
                    Name = "Barn1",
                    Households = new List<Household>
                                                    {
                                                        new Household
                                                            {
                                                                Address = "123 Main St",
                                                                Members = new List<Member> {new Member {Name = "Joe"},},
                                                            },
                                                    }
                };
                using (var session = store.OpenSession())
                {
                    session.Store(barn1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics statistics;

                    // Query using dynamic index
                    var result1 = from b in session.Query<Barn>()
                                    .Customize(x => x.WaitForNonStaleResults())
                                    .Statistics(out statistics)
                                  where b.Households.Any(h => h.Members.Any(m => m.Name == "Joe"))
                                  select b;
                    var result1List = result1.ToList();
                    Assert.Equal(1, result1List.Count());

                    // Query using BarnIndex
                    var result2 = from b in session.Query<Barn, BarnIndex>()
                                    .Customize(x => x.WaitForNonStaleResults())
                                    .Statistics(out statistics)
                                  where b.Name == "Barn1"
                                  select b;
                    var result2List = result2.ToList();
                    var indexName2 = statistics.IndexName;
                    Assert.Equal("BarnIndex", indexName2);
                    Assert.Equal(1, result2List.Count());

                    // Query using BarnIndex
                    var result3 = from b in session.Query<Barn, BarnIndex>()
                                    .Customize(x => x.WaitForNonStaleResults())
                                    .Statistics(out statistics)
                                  where b.Households.Any(h => h.Members.Any(m => m.Name == "Joe"))
                                  select b;
                    var result3List = result3.ToList();
                    var indexName3 = statistics.IndexName;
                    Assert.Equal("BarnIndex", indexName3);

                    // Test fails here!!
                    Assert.Equal(1, result3List.Count());
                }
            }
        }

        private class Barn
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
            public IList<Household> Households { get; set; }
        }

        private class Household
        {
            public string InternalId { get; set; }
            public string Address { get; set; }
            public IList<Member> Members { get; set; }
        }

        private class Member
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
        }
    }
}
