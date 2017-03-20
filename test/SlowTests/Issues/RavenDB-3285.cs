using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3285 : RavenTestBase
    {
        [Fact]
        public void LockIndex()
        {
            using (var store = GetDocumentStore())
            {

                var index = new IndexEmployee
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);

                store.Admin.Send(new SetIndexLockOperation("IndexEmployee", IndexLockMode.Unlock));
                var indexDefinition = store.Admin.Send(new GetIndexOperation("IndexEmployee"));
                Assert.Equal(IndexLockMode.Unlock, indexDefinition.LockMode);

                store.Admin.Send(new SetIndexLockOperation("IndexEmployee", IndexLockMode.LockedError));
                indexDefinition = store.Admin.Send(new GetIndexOperation("IndexEmployee"));
                Assert.Equal(IndexLockMode.LockedError, indexDefinition.LockMode);

                store.Admin.Send(new SetIndexLockOperation("IndexEmployee", IndexLockMode.LockedIgnore));
                indexDefinition = store.Admin.Send(new GetIndexOperation("IndexEmployee"));
                Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition.LockMode);

                store.Admin.Send(new SetIndexLockOperation("IndexEmployee", IndexLockMode.LockedIgnore));
                indexDefinition = store.Admin.Send(new GetIndexOperation("IndexEmployee"));
                Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition.LockMode);
            }
        }

        private class IndexEmployee : AbstractIndexCreationTask<Employee>
        {
            public IndexEmployee()
            {
                Map = employees =>
                    from employee in employees
                    select new
                    {
                        employee.FirstName,
                        EmployeeHomeAddress = employee.HomeAddress,
                    };
            }
        }

        private class Employee
        {
            public string FirstName { get; set; }
            public string HomeAddress { get; set; }
        }
    }
}
