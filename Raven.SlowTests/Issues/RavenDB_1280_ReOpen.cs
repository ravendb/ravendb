// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1280_ReOpen.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Common;
using Raven.Tests.Common.Attributes;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_1280_ReOpen : RavenTest
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.MaxNumberOfItemsToIndexInSingleBatch = 50;
            configuration.MaxNumberOfItemsToReduceInSingleBatch = 50;
        }

        //[Theory]
        //[PropertyData("Storages")]
        [TimeBombedFact(2014, 4, 30, "Performance issue, Pawel investigating this")]
		public void Can_Index_With_Missing_LoadDocument_References(string storageTypeName)
        {
            const int iterations = 8000;
	        var sp = Stopwatch.StartNew();
			using (var store = NewRemoteDocumentStore(requestedStorage: storageTypeName))
            {
                new EmailIndex().Execute(store);

                Parallel.For(0, iterations, i =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new EmailDocument {Id = "Emails/" + i, To = "root@localhost", From = "nobody@localhost", Subject = "Subject" + i});
                        session.SaveChanges();
                    }
                });
                

                // Test that the indexing can complete, without being in an infinite indexing run due to touches to documents increasing the etag.
				WaitForIndexing(store, timeout: Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(sp.Elapsed.TotalSeconds / 2));
            }
        }

        public class EmailIndex : AbstractIndexCreationTask<EmailDocument, EmailIndexDoc>
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

        public class EmailDocument
        {
            public string Id { get; set; }
            public string To { get; set; }
            public string From { get; set; }
            public string Subject { get; set; }
        }

        public class EmailText
        {
            public string Id { get; set; }
            public string Body { get; set; }
        }

        public class EmailIndexDoc
        {
            public string Id { get; set; }
            public string To { get; set; }
            public string From { get; set; }
            public string Subject { get; set; }
            public string Body { get; set; }
        }
    }
}