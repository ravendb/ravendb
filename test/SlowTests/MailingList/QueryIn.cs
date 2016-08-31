// -----------------------------------------------------------------------
//  <copyright file="QueryIn.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class QueryIn : RavenTestBase
    {
        [Fact]
        public void ShouldWork()
        {
            var idents = new[] { 1, 2, 3, 4, 5, 6, 7 };
            var index = 0;

            using (var store = GetDocumentStore())
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

                store.DatabaseCommands.PutIndex("TestIndex", new IndexDefinition
                {
                    Maps = {
                        @"docs.MyEntities.Select(entity => new {
                                    Text = entity.Text,
                                    ImageId = entity.ImageId
                                })"
                    },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "Text", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed } }
                    }
                });

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session
                                        .Query<MyEntity>("TestIndex")
                                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
                                        .Where(x => x.ImageId.In(new[] { 67, 66, 78, 99, 700, 6 })));
                    Assert.NotEmpty(session
                                            .Query<MyEntity>("TestIndex")
                                            .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
                                            .Where(x => x.ImageId.In(new[] { 67, 23, 66, 78, 99, 700, 6 })));
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
