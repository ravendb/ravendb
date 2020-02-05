using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RavenDB_10506 : RavenTestBase
    {
        public RavenDB_10506(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Projections_with_multiple_Loads_using_complex_id_reference()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Rule
                    {
                        Name = "Thou shalt have no other gods before me"
                    });
                    session.Store(new SomethingElse
                    {
                        Name = "Jerry Garcia"
                    }, "somethings/1-A");
                    session.Store(new Package
                    {
                        SomethingElseReference = "somethings/1-A",
                        ComplexRuleReference = new ComplexRuleReference
                        {
                            Id = "rules/1-A"
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var projection = from package in session.Query<Package>()
                                     let somethingElse = RavenQuery.Load<SomethingElse>(package.SomethingElseReference)
                                     let rule = RavenQuery.Load<Rule>(package.ComplexRuleReference.Id) //COMPLEX ID REFERENCE LAST
                                     select new
                                     {
                                         PackageId = package.Id,
                                         RuleName = rule.Name,
                                         SomethingElseName = somethingElse.Name
                                     };

                    Assert.Equal("from 'Packages' as package " +
                                 "load package.SomethingElseReference as somethingElse, package.ComplexRuleReference.Id as rule " +
                                 "select { PackageId : id(package), RuleName : rule.Name, SomethingElseName : somethingElse.Name }"
                                , projection.ToString());

                    var result = projection.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("packages/1-A", result[0].PackageId);
                    Assert.Equal("Thou shalt have no other gods before me", result[0].RuleName);
                    Assert.Equal("Jerry Garcia", result[0].SomethingElseName);

                }
            }
        }

        private class Package
        {
            public string Id { get; set; }
            public ComplexRuleReference ComplexRuleReference { get; set; }
            public string SomethingElseReference { get; set; }
        }
        private class SomethingElse
        {
            public string Name { get; set; }
        }
        private class ComplexRuleReference
        {
            public string Id { get; set; }
        }
        private class Rule
        {
            public string Name { get; set; }
        }
    }
}
