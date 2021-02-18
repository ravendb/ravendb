using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16236 : RavenTestBase
    {
        public RavenDB_16236(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Index_In_Map_Reduce_Nested_Json_Values_That_Are_In_Array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Hibernating Rhinos",
                        Addresses = new List<Address> {
                            new Address
                            {
                                City = "Hadera",
                                Country = "Israel"
                            }
                        }
                    });

                    session.SaveChanges();
                }

                new Companies_Count().Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private class Companies_Count : AbstractIndexCreationTask<Company, Companies_Count.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public int Count { get; set; }
            }

            public Companies_Count()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       Name = company.Name,
                                       Count = 1,
                                       _ = company.Addresses.Select(x => CreateField("Nested", new { Name = x.City }))
                                   };

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count),
                                        _ = new[] { CreateField("Nested", new { Name = g.Key }) },
                                    };
            }
        }

        private class Company
        {
            public string Name { get; set; }

            public List<Address> Addresses { get; set; } = new List<Address>();
        }
    }
}
