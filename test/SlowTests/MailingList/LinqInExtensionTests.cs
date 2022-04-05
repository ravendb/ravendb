using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LinqInExtensionTests : RavenTestBase
    {
        public LinqInExtensionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InListOver256Chars(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var nameList = new List<string>();
                    var count = 0;

                    while (count < 0x100)
                    {
                        var doc = new TestDoc { Name = Convert.ToBase64String(Guid.NewGuid().ToByteArray()) };
                        session.Store(doc);

                        nameList.Add(doc.Name);
                        count += (doc.Name.Length + 1);
                    }
                    session.SaveChanges();

                    var foundDocs = session.Query<TestDoc>()
                                           .Customize(x => x.WaitForNonStaleResults())
                                           .Where(doc => doc.Name.In(nameList)).ToList();

                    Assert.Equal(nameList.Count, foundDocs.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InListOver256Chars2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var nameList = new List<string>();
                    var count = 0;
                    var index = 0;
                    while (count < 0x100)
                    {
                        var doc = new TestDoc { Name = new string('a', 300) + index };
                        session.Store(doc);

                        nameList.Add(doc.Name);
                        count += (doc.Name.Length + 1);
                        index++;
                    }
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var foundDocs = session.Query<TestDoc>()
                                           .Customize(x => x.WaitForNonStaleResults())
                                           .Where(doc => doc.Name.In(nameList)).ToList();

                    Assert.Equal(nameList.Count, foundDocs.Count);
                }
            }
        }

        private class TestDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
