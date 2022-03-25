using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3285 : RavenTestBase
    {
        public RavenDB_3285(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LockIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new IndexEmployee
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);

                var tester = store.Maintenance.ForTesting(() => new GetIndexOperation("IndexEmployee"));

                store.Maintenance.Send(new SetIndexesLockOperation("IndexEmployee", IndexLockMode.Unlock));
                tester.AssertAll((_, indexDefinition) => Assert.Equal(IndexLockMode.Unlock, indexDefinition.LockMode));

                store.Maintenance.Send(new SetIndexesLockOperation("IndexEmployee", IndexLockMode.LockedError));
                tester.AssertAll((_, indexDefinition) => Assert.Equal(IndexLockMode.LockedError, indexDefinition.LockMode));

                store.Maintenance.Send(new SetIndexesLockOperation("IndexEmployee", IndexLockMode.LockedIgnore));
                tester.AssertAll((_, indexDefinition) => Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition.LockMode));

                store.Maintenance.Send(new SetIndexesLockOperation("IndexEmployee", IndexLockMode.LockedIgnore));
                tester.AssertAll((_, indexDefinition) => Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition.LockMode));
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
