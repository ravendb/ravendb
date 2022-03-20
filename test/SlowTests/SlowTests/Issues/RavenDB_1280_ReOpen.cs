// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1280_ReOpen.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_1280_ReOpen : RavenTestBase
    {
        public RavenDB_1280_ReOpen(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Index_With_Missing_LoadDocument_References()
        {
            const int iterations = 8000;
            var sp = Stopwatch.StartNew();
            using (var store = GetDocumentStore())
            {
                new EmailIndex().Execute(store);

                Parallel.For(0, iterations, RavenTestHelper.DefaultParallelOptions, i =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new EmailDocument { Id = "Emails/" + i, To = "root@localhost", From = "nobody@localhost", Subject = "Subject" + i });
                        session.SaveChanges();
                    }
                });

                // Test that the indexing can complete, without being in an infinite indexing run due to touches to documents increasing the etag.
                Indexes.WaitForIndexing(store, timeout: Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(Math.Max(sp.Elapsed.TotalSeconds / 2, 120)));
            }
        }

        private class EmailIndex : AbstractIndexCreationTask<EmailDocument, EmailIndexDoc>
        {
            public EmailIndex()
            {
                Map = emails => from email in emails
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
