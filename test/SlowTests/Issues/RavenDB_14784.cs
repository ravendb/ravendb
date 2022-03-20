using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14784 : RavenTestBase
    {
        public RavenDB_14784(ITestOutputHelper output)
            : base(output)
        {
        }

        private class Companies_With_Attachments : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string CompanyName { get; set; }

                public string AttachmentName { get; set; }

                public string AttachmentContentType { get; set; }

                public string AttachmentHash { get; set; }

                public long AttachmentSize { get; set; }

                public string AttachmentContent { get; set; }

                public Stream AttachmentContentStream { get; set; }
            }

            public Companies_With_Attachments()
            {
                Map = companies => from company in companies
                                   let attachment = LoadAttachment(company, company.ExternalId)
                                   select new Result
                                   {
                                       CompanyName = company.Name,
                                       AttachmentName = attachment.Name,
                                       AttachmentContentType = attachment.ContentType,
                                       AttachmentHash = attachment.Hash,
                                       AttachmentSize = attachment.Size,
                                       AttachmentContent = attachment.GetContentAsString(Encoding.UTF8),
                                       AttachmentContentStream = attachment.GetContentAsStream()
                                   };

                Store(nameof(Result.AttachmentContentStream), FieldStorage.Yes);
            }
        }

        private class Companies_With_Attachments_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Companies_With_Attachments_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    @"map('Companies', function (company) {
var attachment = loadAttachment(company, company.ExternalId);
return {
    CompanyName: company.Name,
    AttachmentName: attachment.Name,
    AttachmentContentType: attachment.ContentType,
    AttachmentHash: attachment.Hash,
    AttachmentSize: attachment.Size,
    AttachmentContent: attachment.getContentAsString('utf8')
};
})"
                };
            }
        }

        private class Companies_With_Multiple_Attachments : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string CompanyName { get; set; }

                public string AttachmentName { get; set; }

                public string AttachmentContentType { get; set; }

                public string AttachmentHash { get; set; }

                public long AttachmentSize { get; set; }

                public string AttachmentContent { get; set; }
            }

            public Companies_With_Multiple_Attachments()
            {
                Map = companies => from company in companies
                                   let attachments = LoadAttachments(company)
                                   from attachment in attachments
                                   select new Result
                                   {
                                       CompanyName = company.Name,
                                       AttachmentName = attachment.Name,
                                       AttachmentContentType = attachment.ContentType,
                                       AttachmentHash = attachment.Hash,
                                       AttachmentSize = attachment.Size,
                                       AttachmentContent = attachment.GetContentAsString(Encoding.UTF8)
                                   };
            }
        }

        private class Companies_With_Multiple_Attachments_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Companies_With_Multiple_Attachments_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    @"map('Companies', function (company) {
var attachments = loadAttachments(company);
return attachments.map(attachment => ({
    CompanyName: company.Name,
    AttachmentName: attachment.Name,
    AttachmentContentType: attachment.ContentType,
    AttachmentHash: attachment.Hash,
    AttachmentSize: attachment.Size,
    AttachmentContent: attachment.getContentAsString('utf8')
}));
})"
                };
            }
        }

        [Fact]
        public void Can_Index_Attachments()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_With_Attachments();
                index.Execute(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", ExternalId = "file.txt" }, "companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("hr", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_World>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_world>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Cosmos>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                // live update
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Moon>")), "application/text");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("12", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_moon>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void Can_Index_Multiple_Attachments()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_With_Multiple_Attachments();
                index.Execute(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_World>")), "application/text");
                    session.Advanced.Attachments.Store(company, "file2.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Aloha_World>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_world>", terms);
                Assert.Contains("<aloha_world>", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Cosmos>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("13", terms);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);
                Assert.Contains("<aloha_world>", terms);

                // live update
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file2.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Aloha_Moon>")), "application/text");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("12", terms);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);
                Assert.Contains("<aloha_moon>", terms);
            }
        }

        [Fact]
        public void Can_Index_Attachments_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_With_Attachments_JavaScript();
                index.Execute(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", ExternalId = "file.txt" }, "companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("hr", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_World>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_world>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Cosmos>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);

                // live update
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Moon>")), "application/text");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("file.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.NotNull(terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("12", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("<hello_moon>", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentStream), fromValue: null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void Can_Index_Multiple_Attachments_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_With_Multiple_Attachments_JavaScript();
                index.Execute(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.False(staleness.IsStale);
                Assert.Equal(0, staleness.StalenessReasons.Count);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_World>")), "application/text");
                    session.Advanced.Attachments.Store(company, "file2.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Aloha_World>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_world>", terms);
                Assert.Contains("<aloha_world>", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Hello_Cosmos>")), "application/text");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(index.IndexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("13", terms);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);
                Assert.Contains("<aloha_world>", terms);

                // live update
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Advanced.Attachments.Store(company, "file2.txt", new MemoryStream(Encoding.UTF8.GetBytes("<Aloha_Moon>")), "application/text");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.CompanyName), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("hr", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentName), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("file.txt", terms);
                Assert.Contains("file2.txt", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContentType), fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("application/text", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentHash), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.NotNull(terms[0]);
                Assert.NotNull(terms[1]);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentSize), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("12", terms);
                Assert.Contains("14", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Companies_With_Attachments.Result.AttachmentContent), fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("<hello_cosmos>", terms);
                Assert.Contains("<aloha_moon>", terms);
            }
        }
    }
}
