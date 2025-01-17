using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23556 : RavenTestBase
{
    public RavenDB_23556(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Test()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Name = "very cool name" };
                
                session.Store(dto);
                
                session.SaveChanges();
            }
            
            Etl.AddEtl(store,
                new AiEtlConfiguration()
                {
                    Name = "Cool Test",
                    ConnectionStringName = "abc",
                    Transforms = new List<Transformation>() { new Transformation() { Collections = new List<string>() { "Dtos" }, Name = "CoolName2", Script = "loadToWhatever(){}" } },
                    FieldsToInclude = new List<string>() { "Name" }
                }, new AiConnectionString() { Name = "ddd" });
            
            WaitForUserToContinueTheTest(store);
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}
