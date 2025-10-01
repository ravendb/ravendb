using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22679_GH_Discussions_18969 : RavenTestBase
{
    public RavenDB_22679_GH_Discussions_18969(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestDoubleWhereClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var p1 = new PortalUser() { IsActive = true, IsArchived = false };
                
                session.Store(p1);
                
                session.SaveChanges();

                var portalUserIds = new List<string> { p1.Id };
                
                var portalUsersAll = session.Query<PortalUser>()
                    .Where(x=> x.Id.In(portalUserIds))
                    .Where(x=> x.IsActive == true && x.IsArchived == false)
                    .ToList();
                
                Assert.Equal(1, portalUsersAll.Count);
            }
        }
    }
    
    private class PortalUser
    {
        public string Id { get; set; }
        public bool IsActive { get; set; }
        public bool IsArchived { get; set; }
    }
}
