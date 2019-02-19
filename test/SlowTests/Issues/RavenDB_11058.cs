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
        public void CanCopyAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");

                    session.Advanced.Attachments.Store(company, "file1", new MemoryStream(Encoding.UTF8.GetBytes("123")));
                    session.Advanced.Attachments.Store(company, "file10", new MemoryStream(Encoding.UTF8.GetBytes("321")));

                    session.SaveChanges();
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    var newCompany = new Company
                    {
                        Name = "CF"
                    };

                    session.Store(newCompany, "companies/2");

                    var oldCompany = session.Load<Company>("companies/1");

                    session.Advanced.Attachments.Copy(oldCompany, "file1", newCompany, "file2");

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file1"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file2"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file10"));

                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file1"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file2"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file10"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Copy("companies/1", "file1", "companies/2", "file3");

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(4, stats.CountOfAttachments);
                Assert.Equal(2, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file1"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file2"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file3"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file10"));

                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file1"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file2"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file3"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file10"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Copy("companies/1", "file1", "companies/2", "file3"); // should throw

                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void CanMoveAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");

                    session.Advanced.Attachments.Store(company, "file1", new MemoryStream(Encoding.UTF8.GetBytes("123")));
                    session.Advanced.Attachments.Store(company, "file10", new MemoryStream(Encoding.UTF8.GetBytes("321")));
                    session.Advanced.Attachments.Store(company, "file20", new MemoryStream(Encoding.UTF8.GetBytes("456")));

                    session.SaveChanges();
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfAttachments);
                Assert.Equal(3, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    var newCompany = new Company
                    {
                        Name = "CF"
                    };

                    session.Store(newCompany, "companies/2");

                    var oldCompany = session.Load<Company>("companies/1");

                    session.Advanced.Attachments.Move(oldCompany, "file1", newCompany, "file2");

                    session.SaveChanges();

                    //var oldAttachments = session.Advanced.Attachments.GetNames(oldCompany);
                    //Assert.False(oldAttachments.Any(x => x.Name == "file1"));

                    //var newAttachments = session.Advanced.Attachments.GetNames(newCompany);
                    //Assert.True(newAttachments.Any(x => x.Name == "file1"));
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfAttachments);
                Assert.Equal(3, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file1"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file2"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file10"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file20"));

                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file1"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file2"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file10"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file20"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Move("companies/1", "file10", "companies/2", "file3");

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfAttachments);
                Assert.Equal(3, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file1"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file2"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file3"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/1", "file10"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/1", "file20"));

                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file1"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file2"));
                    Assert.True(session.Advanced.Attachments.Exists("companies/2", "file3"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file10"));
                    Assert.False(session.Advanced.Attachments.Exists("companies/2", "file20"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Move("companies/1", "file20", "companies/2", "file3"); // should throw

                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }
            }
        }

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
