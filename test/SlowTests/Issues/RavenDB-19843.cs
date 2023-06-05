using System;
using System.Linq;
using FastTests;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19843 : RavenTestBase
{
    public RavenDB_19843(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {true})]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] {false})]
    public void CheckIfNullDatesAreReturnedLastWithConfigOptionEnabled(Options options, bool configEnabled)
    {
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseRecord = r =>
                   {
                       options.ModifyDatabaseRecord(r);
                       r.Settings[RavenConfiguration.GetKey(x => x.Indexing.OrderByTicksAutomaticallyWhenDatesAreInvolved)] = configEnabled.ToString();
                   }
               }))
        {
            using (var session = store.OpenSession())
            {
                var now = DateTime.UtcNow;

                var dto1 = new DtoWithDate() { CreationTime = now };
                var dto2 = new DtoWithDate() { CreationTime = now.AddHours(2) };
                var dto3 = new DtoWithDate() { };

                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);

                session.SaveChanges();

                var results = session.Query<DtoWithDate>()
                    .OrderByDescending(x => x.CreationTime).ToList();
                
                if (configEnabled)
                    Assert.Equal(null, results[2].CreationTime);
                else
                    Assert.Equal(null, results[0].CreationTime);
            }
        }
    }
}

public class DtoWithDate
{
    public string Id { get; set; }
    public DateTime? CreationTime { get; set; }
}
