using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23076 : RavenTestBase
{
    public RavenDB_23076(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.None)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Test(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var sampleObject = new
                {
                    Vector = new RavenVector<float>(new []{ 0.1f, 0.2f }),
                };
                
                session.Store(sampleObject);
                
                session.SaveChanges();
            }
        }
    }
}
