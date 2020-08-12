using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13732 : RavenTestBase
    {
        public RavenDB_13732(ITestOutputHelper output) : base(output)
        {
        }

        private class AttachmentIndex : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public List<string> AttachmentNames { get; set; }

                public List<string> AttachmentHashes { get; set; }

                public List<string> AttachmentSizes { get; set; }

                public List<string> AttachmentContentTypes { get; set; }
            }

            public AttachmentIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Employees', function (e) {
var attachments = attachmentsFor(e);
var attachmentNames = attachments.map(function(attachment) { return attachment.Name; });
var attachmentHashes = attachments.map(function(attachment) { return attachment.Hash; });
var attachmentSizes = attachments.map(function(attachment) { return attachment.Size; });
var attachmentContentTypes = attachments.map(function(attachment) { return attachment.ContentType; });
return {
    AttachmentNames: attachmentNames,
    AttachmentHashes: attachmentHashes,
    AttachmentSizes: attachmentSizes,
    AttachmentContentTypes: attachmentContentTypes
};
})",
                };
            }
        }

        [Fact]
        public void SupportAttachmentsForInIndex_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                var index = new AttachmentIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "John",
                        LastName = "Doe"
                    }, "employees/1");

                    session.Store(new Employee
                    {
                        FirstName = "Bob",
                        LastName = "Doe"
                    }, "employees/2");

                    session.Store(new Employee
                    {
                        FirstName = "Edward",
                        LastName = "Doe"
                    }, "employees/3");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(0, employees.Count);
                }

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");
                    var employee2 = session.Load<Employee>("employees/2");

                    session.Advanced.Attachments.Store(employee1, "photo.jpg", new MemoryStream(Encoding.UTF8.GetBytes("123")), "image/jpeg");
                    session.Advanced.Attachments.Store(employee1, "cv.pdf", new MemoryStream(Encoding.UTF8.GetBytes("321")), "application/pdf");

                    session.Advanced.Attachments.Store(employee2, "photo.jpg", new MemoryStream(Encoding.UTF8.GetBytes("456789")), "image/jpeg");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(2, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));

                    employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("cv.pdf"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("John", employees.Select(x => x.FirstName));
                }

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(AttachmentIndex.Result.AttachmentNames), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("photo.jpg", terms);
                Assert.Contains("cv.pdf", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(AttachmentIndex.Result.AttachmentSizes), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("3", terms);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(AttachmentIndex.Result.AttachmentHashes), fromValue: null));
                Assert.Equal(3, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(AttachmentIndex.Result.AttachmentContentTypes), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("image/jpeg", terms);
                Assert.Contains("application/pdf", terms);

                using (var session = store.OpenSession())
                {
                    var employee1 = session.Load<Employee>("employees/1");

                    session.Advanced.Attachments.Delete(employee1, "photo.jpg");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var employees = session.Query<AttachmentIndex.Result, AttachmentIndex>()
                        .Where(x => x.AttachmentNames.Contains("photo.jpg"))
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, employees.Count);
                    Assert.Contains("Bob", employees.Select(x => x.FirstName));
                }
            }
        }
    }
}
