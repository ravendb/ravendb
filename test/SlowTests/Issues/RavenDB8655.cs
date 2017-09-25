using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB8655 : RavenTestBase
    {
        protected List<dynamic> ExecuteQuery(string q)
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new CreateSampleDataOperation());

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
                store.Admin.Send(new CreateSampleDataOperation());

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
        [InlineData(@"from Users
select 1 as B, 2 as B ")]
        [InlineData(@"from Users group by Age 
select count(), Name as Count ")]
        public void DuplicateAliases(string q)
        {
            var iqe = Assert.Throws<InvalidQueryException>(() => ExecuteQuery(q));
            Assert.Contains("Duplicate alias", iqe.Message);
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
