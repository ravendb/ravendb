﻿using System.Collections.Generic;
using System.Globalization;
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

        [Fact]
        public void CanUseTryConvertInIndex_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                var index = new ConvertIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        DblNullValue = 1.1,
                        DblValue = 2.1,
                        FltNullValue = 3.1f,
                        FltValue = 4.1f,
                        IntNullValue = 5,
                        IntValue = 6,
                        LngNullValue = 7,
                        LngValue = 8,
                        ObjValue = new Company { Name = "HR" },
                        StgValue = "str"
                    }, "items/1");

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.DblNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(double.Parse(terms[0], CultureInfo.InvariantCulture) - 1.1 < double.Epsilon);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.DblValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(double.Parse(terms[0], CultureInfo.InvariantCulture) - 2.1 < double.Epsilon);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.FltNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(float.Parse(terms[0], CultureInfo.InvariantCulture) - 3.1f < float.Epsilon);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.FltValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.True(float.Parse(terms[0], CultureInfo.InvariantCulture) - 4.1f < float.Epsilon);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.IntNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("5", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.IntValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("6", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.LngNullValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("7", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.LngValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("8", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.ObjValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("-1", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Item.StgValue), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("-1", terms[0]);
            }
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
})"
                };
            }
        }

        private class ConvertIndex : AbstractJavaScriptIndexCreationTask
        {
            public ConvertIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Items', function (item) {
return {
    DblValue: tryConvertToNumber(item.DblValue) || -1,
    DblNullValue: tryConvertToNumber(item.DblNullValue) || -1,
    FltValue: tryConvertToNumber(item.FltValue) || -1,
    FltNullValue: tryConvertToNumber(item.FltNullValue) || -1,
    LngValue: tryConvertToNumber(item.LngValue) || -1,
    LngNullValue: tryConvertToNumber(item.LngNullValue) || -1,
    IntValue: tryConvertToNumber(item.IntValue) || -1,
    IntNullValue: tryConvertToNumber(item.IntNullValue) || -1,
    StgValue: tryConvertToNumber(item.StgValue) || -1,
    ObjValue: tryConvertToNumber(item.ObjValue) || -1
};
})"
                };
            }
        }

        private class Item
        {
            public double DblValue { get; set; }

            public double? DblNullValue { get; set; }

            public float FltValue { get; set; }

            public float? FltNullValue { get; set; }

            public long LngValue { get; set; }

            public long? LngNullValue { get; set; }

            public int IntValue { get; set; }

            public int? IntNullValue { get; set; }

            public string StgValue { get; set; }

            public Company ObjValue { get; set; }
        }
    }
}
