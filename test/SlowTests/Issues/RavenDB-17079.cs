using System.IO;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
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
        public void RavenAttachmentIssue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var employee = new Employee { Id = "One" };
                    session.Store(employee);
                    session.Advanced.Attachments.Store(employee, "file1", new MemoryStream(), "text/plain");
                    session.Advanced.Attachments.Store(employee, "file2", new MemoryStream(), "text/plain");
                    session.Advanced.Attachments.Store(employee, "file3", new MemoryStream(), "text/plain");
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    
                    var employee = session.Load<Employee>("One");
                    var attachmentNames1 = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(3, attachmentNames1.Length);
                    
                    session.Advanced.Attachments.Delete(employee, "file3");
                    session.SaveChanges();
                    
                    var attachmentNames = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(2, attachmentNames.Length);
                }

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("One");
                    var attachmentNames = session.Advanced.Attachments.GetNames(employee);
                    Assert.Equal(2, attachmentNames.Length);
                }
                WaitForUserToContinueTheTest(store);

            }
        }

        public class Employee
        {
            public string Id { get; set; }
        }
    }
}
