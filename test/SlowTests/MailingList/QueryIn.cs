// -----------------------------------------------------------------------
//  <copyright file="QueryIn.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class QueryIn : RavenTestBase
    {
        public QueryIn(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldWork(Options options)
        {
            var idents = new[] { 1, 2, 3, 4, 5, 6, 7 };
            var index = 0;

            using (var store = GetDocumentStore(options))
            {
                for (var i = 0; i < 64; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        for (var j = 0; j < 10; j++)
                            session.Store(new MyEntity
                            {
                                ImageId = idents[index++ % idents.Length],
                            });
                        session.SaveChanges();
                    }
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {  new IndexDefinition
                {
                    Name = "TestIndex",
                    Maps = {
                        @"docs.MyEntities.Select(entity => new {
                                    Text = entity.Text,
                                    ImageId = entity.ImageId
                                })"
                    },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Text", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                    }
                }}));

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session
                        .Query<MyEntity>("TestIndex")
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
                        .Search(x => x.ImageId, "67 66 78 99 700 6")
                        .Take(1024));
                    Assert.NotEmpty(session
                        .Query<MyEntity>("TestIndex")
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
                        .Search(x => x.ImageId, " 67 23 66 78 99 700 6")
                        .Take(1024));
                }
            }
        }

        private class MyEntity
        {
            public string Id { get; set; }
            public int ImageId { get; set; }
            public string Text { get; set; }
        }

    }
}
