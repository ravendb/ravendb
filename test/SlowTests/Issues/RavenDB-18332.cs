using System;
using System.Linq;
using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18332 : RavenTestBase
{
    public RavenDB_18332(ITestOutputHelper output) : base(output)
    {
    }

    private class User
    {
        public string Id, Name, CompanyId;
    }

    [Fact]
    public void WillNotGetIndexErrorOnFirstOrDefaultWithNoValues()
    {
        var store = GetDocumentStore();
        var indexDef = new IndexDefinition
        {
            Maps =
            {
                @"from u in docs.Users 
                select new
                {
                    Name = (string)null,
                    Id = u.CompanyId,
                    Employees = 1
                }",
                @"from c in docs.Companies
                select new
                {
                    c.Name,
                    c.Id,
                    Employees = 0
                }"
            },
            Reduce = @"from r in results
                group r by r.Id
                into g
                select new { Id = g.Key, g.FirstOrDefault(x => x.Name != null).Name, Employees = g.Sum(x => x.Employees) };",
            Name = "MyIndex"
        };
        store.Maintenance.Send(new PutIndexesOperation(indexDef));

        using (var s = store.OpenSession())
        {
            s.Store(new User
            {
                Name = "Oren",
                CompanyId = "companies/1"
            });
            s.SaveChanges();
        }
        
        WaitForIndexing(store);
        
        using (var s = store.OpenSession())
        {
            int count = s.Query<dynamic>("MyIndex").Count();
            Assert.Equal(1, count);
        }


    }
}
