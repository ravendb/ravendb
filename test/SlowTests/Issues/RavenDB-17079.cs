using System.IO;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17079 : RavenTestBase
    {
        public RavenDB_17079(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void Should_Return_Appropriate_Number_Of_Attachments_After_Deletion()
        {
            using (var store = GetDocumentStore())
            {
                var employee = new Employee { Id = "One" };
                using (var session = store.OpenSession())
                {
                    session.Store(employee);
                    session.Advanced.Attachments.Store(employee, "file1", new MemoryStream(), "text/plain");
                    session.Advanced.Attachments.Store(employee, "file2", new MemoryStream(), "text/plain");
                    session.Advanced.Attachments.Store(employee, "file3", new MemoryStream(), "text/plain");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                { 
                    employee = session.Load<Employee>(employee.Id);
                    var attachmentNames1 = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(3, attachmentNames1.Length);
                    
                    session.Advanced.Attachments.Delete(employee, "file3");
                    session.SaveChanges();
                    
                    var attachmentNames = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(2, attachmentNames.Length);
                }

                using (var session = store.OpenSession())
                {
                    employee = session.Load<Employee>(employee.Id);
                    var attachmentNames = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(2, attachmentNames.Length);
                }
            }
        }

    }
}
