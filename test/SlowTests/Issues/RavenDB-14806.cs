using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14806 : RavenTestBase
    {
        public RavenDB_14806(ITestOutputHelper output) : base(output)
        {
        }

        public class ContractClause
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public string CompanyId { get; set; }
            public string Group { get; set; }
            public int SortOrder { get; set; }
        }

        [Fact]
        public async Task CanQueryOverReservedPropertieS()
        {
            using var store = GetDocumentStore();

            using var session = store.OpenAsyncSession();
            string companyId = "companies/1-A";

            await session.Query<ContractClause>()
                .Where(c => c.CompanyId == companyId)
                .OrderBy(c => c.Group)
                .ThenBy(c => c.SortOrder)
                .Select(c => new ContractClauseListItem
                {
                    Id = c.Id,
                    Name = c.Name,
                    Group = c.Group,
                    SortOrder = c.SortOrder,
                    Description = c.Description
                })
                .ToListAsync();
        }

        public class ContractClauseListItem
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public string Group { get; set; }
            public int SortOrder { get; set; }
        }
    }
}
