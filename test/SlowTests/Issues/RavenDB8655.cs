using Tests.Infrastructure;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB8655 : RavenTestBase
    {
        public RavenDB8655(ITestOutputHelper output) : base(output)
        {
        }

        protected List<dynamic> ExecuteQuery(string q)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    return session.Advanced.RawQuery<dynamic>(q)
                        .ToList();
                }
            }
        }

        protected void ExecutePatch(string q)
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                store.Operations.Send(new PatchByQueryOperation(q));
            }
        }

        [Fact]
        public void ProjectFullDocumentWithAlias()
        {
            var results = ExecuteQuery("from Employees e select e");
            Assert.Equal(9, results.Count);
        }

        [Theory]
        [InlineData(@"from Orders 
select count()", "count may only be used in group by queries")]
        [InlineData(@"from Orders 
group by ShippedAt
order by count() asc
select id()", "Cannot use id() method in a group by query")]
        [InlineData(@"from Users
select 1 as B, 2 as B ", "Duplicate alias")]
        [InlineData(@"from Users group by Age 
select count(), Name as Count ", "Duplicate alias")]
        public void InvalidQuery(string q, string err)
        {
            var iqe = Assert.Throws<InvalidQueryException>(() => ExecuteQuery(q));
            Assert.Contains(err, iqe.Message);
        }


        [Fact]
        public void PatchWithLoad()
        {
            ExecutePatch(@"from Employees e
load e.ReportsTo  r
update {
    e.ManagerName = r.FirstName;
}");
        }

        [Fact]
        public void UseCountInOrderByAndNonInSelect()
        {
            ExecuteQuery(@"from Orders 
group by ShippedAt
order by count() asc
select key()");
        }

        [Fact]
        public void ProjectReferenceWithAliasFromLoad()
        {
            var results = ExecuteQuery(@"from Employees as e
load e.ReportsTo as r
select r");
            Assert.Equal(9, results.Count);
        }

        [Fact]
        public void ProjectDocumentReferenceWithAlias()
        {
            var results = ExecuteQuery(@"
from Employees  e
load e.ReportsTo as r
select r, e");
            Assert.Equal(9, results.Count);
        }


    }
}
