using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22862 : RavenTestBase
{
    public RavenDB_22862(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIfSideBySideAutoIndexResetThrowsException()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                const string indexName = "Auto/Dtos/ByName";
                
                _ = session.Query<Dto>().Where(x => x.Name == "abc").ToList();
                    
                var exception = Assert.Throws<RavenException>(() => store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, indexResetMode: IndexResetMode.SideBySide)).ExecuteOnAll());

                Assert.IsType<NotSupportedException>(exception.InnerException);
                Assert.Contains("Side by side index reset is not supported for auto indexes.", exception.InnerException.Message);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}
