
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3285 : RavenTestBase
    {
        [Fact]
        public void LockIndex()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {

                var index = new IndexEmployee
                {
                    Conventions = new DocumentConvention()
                };
                index.Execute(store);

               store.DatabaseCommands.SetIndexLock("IndexEmployee", IndexLockMode.Unlock);
                 var indexDefinition = store.DatabaseCommands.GetIndex("IndexEmployee");
                Assert.Equal(IndexLockMode.Unlock, indexDefinition.LockMode);

                store.DatabaseCommands.SetIndexLock("IndexEmployee", IndexLockMode.LockedError);
                var indexDefinition1 = store.DatabaseCommands.GetIndex("IndexEmployee");
                Assert.Equal(IndexLockMode.LockedError, indexDefinition1.LockMode);

                store.DatabaseCommands.SetIndexLock("IndexEmployee", IndexLockMode.LockedIgnore);
                var indexDefinition2 = store.DatabaseCommands.GetIndex("IndexEmployee");
                Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition2.LockMode);

                store.DatabaseCommands.SetIndexLock("IndexEmployee", IndexLockMode.LockedIgnore);
                var indexDefinition3 = store.DatabaseCommands.GetIndex("IndexEmployee");
                Assert.Equal(IndexLockMode.LockedIgnore, indexDefinition3.LockMode);


                store.DatabaseCommands.SetIndexLock("IndexEmployee", IndexLockMode.SideBySide);
                var indexDefinition4 = store.DatabaseCommands.GetIndex("IndexEmployee");
                Assert.Equal(IndexLockMode.SideBySide, indexDefinition4.LockMode);

            }
        }

        public class IndexEmployee : AbstractIndexCreationTask<Employee>
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

        public class Employee
        {
            public string FirstName { get; set; }
            public string HomeAddress { get; set; }
        }
    }
}