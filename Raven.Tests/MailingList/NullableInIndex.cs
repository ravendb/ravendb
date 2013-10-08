// -----------------------------------------------------------------------
//  <copyright file="NullableInIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public sealed class NullableInIndex : RavenTestBase
    {
        public sealed class Document
        {
            public TimeSpan? Time
            {
                get;
                set;
            }
        }

        public sealed class Documents_ByTime : AbstractIndexCreationTask<Document>
        {
            public Documents_ByTime()
            {
                Map = documents => from d in documents
                                   where d.Time.HasValue
                                   select new
                                   {
                                       Seconds = d.Time.Value.TotalSeconds
                                   };
            }
        }


        [Fact]
        public void Works()
        {
            using (var store = NewDocumentStore())
            {
                Assert.DoesNotThrow(() => new Documents_ByTime().Execute(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Document{Time = TimeSpan.FromSeconds(3)});
                    session.Store(new Document { Time = null });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var x = session.Query<Document, Documents_ByTime>()
                           .Count();
                    Assert.Equal(1, x);
                }

                Assert.Empty(store.DocumentDatabase.Statistics.Errors);
            }
        }
    }
}