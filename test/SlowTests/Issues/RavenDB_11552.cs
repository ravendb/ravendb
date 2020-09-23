using System.IO;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11552 : RavenTestBase
    {
        public RavenDB_11552(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PatchWillUpdateTrackedDocumentAfterSaveChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.Advanced.Patch(company, c => c.Name, "CF");

                    var cv = session.Advanced.GetChangeVectorFor(company);
                    var lastModified = session.Advanced.GetLastModifiedFor(company);

                    session.SaveChanges();

                    Assert.Equal("CF", company.Name);

                    Assert.NotEqual(cv, session.Advanced.GetChangeVectorFor(company));
                    Assert.NotEqual(lastModified, session.Advanced.GetLastModifiedFor(company));

                    company.Fax = "123";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    Assert.Equal("CF", company.Name);
                    Assert.Equal("123", company.Fax);
                }
            }
        }

        [Fact]
        public void DeleteWillWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    Assert.NotNull(company);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Defer(new DeleteCommandData("companies/1", null));

                    session.SaveChanges();

                    Assert.False(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    company = session.Load<Company>("companies/1");

                    Assert.Null(company);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void PatchWillWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    Assert.NotNull(company);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Defer(new PatchCommandData("companies/1", null, new PatchRequest
                    {
                        Script = "this.Name = 'HR2';"
                    }, null));

                    session.SaveChanges();

                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    var company2 = session.Load<Company>("companies/1");

                    Assert.NotNull(company2);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Equal(company, company2);
                    Assert.Equal("HR2", company2.Name);
                }
            }
        }

        [Fact]
        public void AttachmentPutAndDeleteWillWork()
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

                    session.Advanced.Attachments.Store(company, "file0", new MemoryStream(Encoding.UTF8.GetBytes("123")));

                    session.SaveChanges();

                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company).Length);
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    Assert.NotNull(company);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company).Length);

                    session.Advanced.Defer(new PutAttachmentCommandData("companies/1", "file1", new MemoryStream(Encoding.UTF8.GetBytes("123")), null, null));

                    session.SaveChanges();

                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Equal(2, session.Advanced.Attachments.GetNames(company).Length);

                    session.Advanced.Defer(new DeleteAttachmentCommandData("companies/1", "file1", null));

                    session.SaveChanges();

                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company).Length);
                }
            }
        }

        [Fact]
        public void AttachmentCopyAndMoveWillWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company1 = new Company
                    {
                        Name = "HR"
                    };

                    var company2 = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company1, "companies/1");
                    session.Store(company2, "companies/2");

                    session.Advanced.Attachments.Store(company1, "file1", new MemoryStream(Encoding.UTF8.GetBytes("123")));

                    session.SaveChanges();

                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company1).Length);
                }

                using (var session = store.OpenSession())
                {
                    var company1 = session.Load<Company>("companies/1");
                    var company2 = session.Load<Company>("companies/2");

                    Assert.NotNull(company1);
                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company1).Length);

                    Assert.NotNull(company2);
                    Assert.True(session.Advanced.IsLoaded("companies/2"));
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Equal(0, session.Advanced.Attachments.GetNames(company2).Length);

                    session.Advanced.Defer(new CopyAttachmentCommandData("companies/1", "file1", "companies/2", "file1", null));

                    session.SaveChanges();

                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company1).Length);

                    Assert.True(session.Advanced.IsLoaded("companies/2"));
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, session.Advanced.Attachments.GetNames(company2).Length);

                    session.Advanced.Defer(new MoveAttachmentCommandData("companies/1", "file1", "companies/2", "file2", null));

                    session.SaveChanges();

                    Assert.True(session.Advanced.IsLoaded("companies/1"));
                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Equal(0, session.Advanced.Attachments.GetNames(company1).Length);
                    
                    Assert.True(session.Advanced.IsLoaded("companies/2"));
                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Equal(2, session.Advanced.Attachments.GetNames(company2).Length);
                }
            }
        }
    }
}
