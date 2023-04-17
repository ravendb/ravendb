using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18073 : RavenTestBase
{
    public RavenDB_18073(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData("from Dtos where SomeDate < \"2024-02-31T00:00:00.0000000\"")]
    [InlineData("from Dtos where SomeDate between \"2023-01-01T00:00:00.0000000\" and \"2024-15-31T00:00:00.0000000\"")]
    private void CheckIfIncorrectDateIsComparedAsString(string query)
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() { Name = "Name1", SomeDate = "2023-05-14T00:00:00.0000000" };
                var d2 = new Dto() { Name = "Name2", SomeDate = "2023-17-14T00:00:00.0000000" };
                var d3 = new Dto() { Name = "Name3", SomeDate = "2033-04-14T00:00:00.0000000" };
                var d4 = new Dto() { Name = "Name4", SomeDate = "2023-02-35T00:00:00.0000000" };
                
                session.Store(d1);
                session.Store(d2);
                session.Store(d3);
                session.Store(d4);
                
                session.SaveChanges();
                
                var res = session.Advanced.RawQuery<Dto>(query).ToList();
                
                Assert.Equal(3, res.Count);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public string SomeDate { get; set; }
    }
}
