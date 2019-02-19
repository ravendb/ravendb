// -----------------------------------------------------------------------
//  <copyright file="ZNS.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class ZNS : RavenTestBase
    {
        private class TestItem
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public EventDate[] Dates { get; set; }

            public override string ToString()
            {
                return Id;
            }

            public override int GetHashCode()
            {
                return this.Id.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj != null && obj is TestItem)
                    return ((TestItem)obj).Id == this.Id;
                return false;
            }
        }

        private class EventDate
        {
            public DateTime Date { get; set; }
            public string Time { get; set; }

            public override string ToString()
            {
                return Date.ToString("d");
            }
        }

        [Fact]
        public void Can_SortAndPageMultipleDates()
        {
            using (var store = GetDocumentStore())
            {
                //Create an index
                store.Initialize();
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "TestItemsIndex",
                    Maps = { @"from item in docs.TestItems
                        from d in item.Dates.Select((Func<dynamic,dynamic>)(x => x.Date)).Distinct()
                        select new {Id = item.Id, Name = item.Name, EventDate = d}" },
                    Fields =
                    {
                        { "EventDate", new IndexFieldOptions { Storage = FieldStorage.Yes }}
                    }
                }}));

                //Insert some events at random dates
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 50; i++)
                    {
                        var dates = new List<DateTime>();
                        for (var j = 0; j < 5; j++)
                        {
                            var r = new System.Random(i * j);
                            ;
                            dates.Add(new DateTime(2012, r.Next(1, 12), r.Next(1, 31)));
                        }

                        session.Store(new TestItem()
                        {
                            Id = "testitems/" + i,
                            Name = "Event Number " + i,
                            Dates = dates.Select(x => new EventDate() { Date = x }).ToArray()
                        });
                    }
                    session.SaveChanges();
                }

                //Get all results
                QueryStatistics stats;
                List<TestItem> result = null;
                using (var session = store.OpenSession())
                {
                    result = session.Advanced.DocumentQuery<TestItem>("TestItemsIndex")
                        .Statistics(out stats)
                        .WaitForNonStaleResults()
                        //.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-03-01"), DateTime.Parse("2012-06-01"))
                        .OrderBy("EventDate")
                        .Take(1000)
                        .ToList();
                }

                //Get all results, paged
                List<TestItem> pagedResult = new List<TestItem>();
                QueryStatistics stats2;

                int skip = 0;
                var take = 10;
                int page = 0;

                do
                {
                    using (var session = store.OpenSession())
                    {
                        var r = session.Advanced.DocumentQuery<TestItem>("TestItemsIndex")
                            .Statistics(out stats2)
                            //.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-03-01"), DateTime.Parse("2012-06-01"))
                            .OrderBy("EventDate")
                            .Take(take)
                            .Skip((page * take) + skip)
                            .ToList();
                        skip += stats2.SkippedResults;
                        page++;
                        pagedResult.AddRange(r);
                    }

                } while (pagedResult.Count < result.Count);

                Assert.Equal(result.Count, pagedResult.Count);
                //Assert "all" results are equal to paged results
                Assert.Equal(result.Select(x => x.Id).ToArray(), pagedResult.Select(x => x.Id).ToArray());
            }
        }
    }
}
