using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_1280 : RavenNewTestBase
    {
        [Fact]
        public void Referenced_Docs_Are_Indexed_During_Heavy_Writing()
        {
            const int iterations = 6000;

            using (var documentStore = GetDocumentStore())
            {
                var sp = Stopwatch.StartNew();
                Parallel.For(0, iterations, RavenTestHelper.DefaultParallelOptions, i =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new EmailDocument { Id = "Emails/" + i, To = "root@localhost", From = "nobody@localhost", Subject = "Subject" + i });
                        session.SaveChanges();
                    }

                    // ReSharper disable once AccessToDisposedClosure
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new EmailText { Id = "Emails/" + i + "/text", Body = "MessageBody" + i });
                        session.SaveChanges();
                    }
                });


                new EmailIndex().Execute(documentStore);

                var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : sp.Elapsed;
                WaitForIndexing(documentStore, timeout: timeout);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<EmailIndexDoc, EmailIndex>().Count(e => e.Body.StartsWith("MessageBody"));
                    try
                    {
                        Assert.Equal(iterations, results);
                    }
                    catch (Exception ex)
                    {
                        var missingDocs = session.Query<EmailIndexDoc, EmailIndex>().ProjectFromIndexFieldsInto<EmailIndexDoc>()
                                                                                    .Where(e => !e.Body.StartsWith("MessageBody"))
                                                                                    .ToList();
                        Console.WriteLine(string.Join(", ", missingDocs.Select(doc => doc.Id).ToArray()));
                        Console.WriteLine(ex.Message);
                        throw;
                    }
                }
            }
        }

        [Fact]
        public void CanHandleMultipleMissingDocumentsInMultipleIndexes()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new EmailIndex().CreateIndexDefinition();

                for (int i = 0; i < 4; i++)
                {
                    store.Admin.Send(new PutIndexOperation("email" + i, indexDefinition));

                }

                using (var session = store.OpenSession())
                {
                    session.Store(entity: new EmailDocument { });
                    session.Store(entity: new EmailDocument { });
                    session.SaveChanges();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(10));
            }
        }

        private class EmailIndex : AbstractIndexCreationTask<EmailDocument, EmailIndexDoc>
        {
            public EmailIndex()
            {
                Map =
                    emails => from email in emails
                              let text = LoadDocument<EmailText>(email.Id + "/text")
                              select new
                              {
                                  email.To,
                                  email.From,
                                  email.Subject,
                                  Body = text == null ? null : text.Body
                              };
            }
        }

        private class EmailDocument
        {
            public string Id { get; set; }
            public string To { get; set; }
            public string From { get; set; }
            public string Subject { get; set; }
        }

        private class EmailText
        {
            public string Id { get; set; }
            public string Body { get; set; }
        }

        private class EmailIndexDoc
        {
            public string Id { get; set; }
            public string To { get; set; }
            public string From { get; set; }
            public string Subject { get; set; }
            public string Body { get; set; }
        }
    }
}
