using FastTests;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21608 : RavenTestBase
{
    public RavenDB_21608(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void HashIsDifferentForDifferentContainsList()
    {
        var q1 = new IndexQuery
        {
            Query = "from index 'Grade/ByAllIds' where PayriteCode in ($p0)",
            QueryParameters = new Parameters()
            {
                {"p0", new[] { "BFD1", "BDEC", "" } }
            }
        };
            
        var q2 = new IndexQuery
        {
            Query = "from index 'Grade/ByAllIds' where PayriteCode in ($p0)",
            QueryParameters = new Parameters()
            {
                {"p0", new[] { "BFD1", "BDEC" } }
            }
        };

        var q3 = new IndexQuery()
        {
            Query = "from index 'Grade/ByAllIds' where PayriteCode in ($p0)", 
            QueryParameters = new Parameters()
            {
                {"p0", new[] { "BFD1", "BD", "EC" } }
            }
        };

        var q4 = new IndexQuery()
        {
            Query = "from index 'Grade/ByAllIds' where PayriteCode in ($p0)", 
            QueryParameters = new Parameters()
            {
                {"p0", new[] { "BFD1", "BDE", "C" } }
            }
        };
        
        var conventions = new DocumentConventions();
        var jsonSerializer = conventions.Serialization.CreateSerializer();
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            var h1 = q1.GetQueryHash(context, conventions, jsonSerializer);
            var h2 = q2.GetQueryHash(context, conventions, jsonSerializer);
            var h3 = q3.GetQueryHash(context, conventions, jsonSerializer);
            var h4 = q4.GetQueryHash(context, conventions, jsonSerializer);
            
            Assert.NotEqual(h1, h2);
            Assert.NotEqual(h1, h3);
            Assert.NotEqual(h1, h4);
        }
    }
}
