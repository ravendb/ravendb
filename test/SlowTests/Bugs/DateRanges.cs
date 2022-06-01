//-----------------------------------------------------------------------
// <copyright file="DateRanges.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DateRanges : RavenTestBase
    {
        public DateRanges(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryByDate(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Record
                    {
                        Date = new DateTime(2001, 1, 1)
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition{ Maps = {"from doc in docs select new { doc.Date}"},
                    Name = "Date"}}));

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<Record>("Date")
                        .WhereEquals("Date", new DateTime(2001, 1, 1))
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanQueryByDateRange_LowerThan(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Record
                    {
                        Date = new DateTime(2001, 1, 1)
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition { Maps = {"from doc in docs select new { doc.Date}"},
                    Name = "Date"}}));

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<Record>("Date")
                        .WhereLucene("Date", "[* TO " + DateTools.DateToString(new DateTime(2001, 1, 2), DateTools.Resolution.MILLISECOND) + "]")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanQueryByDateRange_GreaterThan(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Record
                    {
                        Date = new DateTime(2001, 1, 1)
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition { Maps = { "from doc in docs select new { doc.Date}" } ,
                    Name = "Date"}}));

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<Record>("Date")
                        .WhereLucene("Date", "[" + DateTools.DateToString(new DateTime(2000, 1, 1), DateTools.Resolution.MILLISECOND) + " TO NULL]")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        private class Record
        {
            public string Id { get; set; }
            public DateTime Date { get; set; }
        }
    }
}
