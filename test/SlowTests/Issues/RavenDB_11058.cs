using System.IO;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11058 : RavenTestBase
    {
        [Fact]
        public void CanRenameAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company);

                    session.Advanced.Attachments.Store(company, "file1", new MemoryStream(Encoding.UTF8.GetBytes("123")));
                    session.Advanced.Attachments.Store(company, "file10", new MemoryStream(Encoding.UTF8.GetBytes("321")));

                    session.SaveChanges();
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1-A");

                    session.Advanced.Attachments.Rename(company, "file1", "file2");

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists("companies/1-A", "file1"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1-A", "file2"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Rename("companies/1-A", "file2", "file3");

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists("companies/1-A", "file2"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1-A", "file3"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Rename("companies/1-A", "file3", "file10"); // should throw

                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);
            }
        }
    }
}
