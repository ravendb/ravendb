using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19522 : RavenTestBase
{
    public RavenDB_19522(ITestOutputHelper output) : base(output)
    {
    }
    
    private class Dto
    {
        public GroupObject Group { get; set; }
    }

    private class GroupObject
    {
        public string Name { get; set; }
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void CheckIfKeywordFirstInNestedPathIsHandledCorrectly()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var g1 = new GroupObject() { Name = "CoolName1" };
                var g2 = new GroupObject() { Name = "CoolName2" };
                var g3 = new GroupObject() { Name = "CoolName3" };

                var d1 = new Dto() { Group = g1 };
                var d2 = new Dto() { Group = g2 };
                var d3 = new Dto() { Group = g3 };
                
                session.Store(d1);
                session.Store(d2);
                session.Store(d3);
                
                session.SaveChanges();
                
                var result = session.Query<Dto>()
                    .Where(dto => dto.Group.Name.In(new string[] { "CoolName1", "CoolName2" })).Customize(x => x.WaitForNonStaleResults()).ToList();
                
                Assert.Equal("CoolName1", result[0].Group.Name);
                Assert.Equal("CoolName2", result[1].Group.Name);
            }
        }
    }
}
