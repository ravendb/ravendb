using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Client
{
    public class QueryDateTime : RavenTestBase
    {
        [Fact]
        public void DynamicQueryingDateTimeShouldWork()
        {
            using (var store = SetupStoreAndPushSomeEntities())
            {
                using (var session = store.OpenSession())
                {
                    var res = session.Query<PersonAndDate>().Where(x => x.Date.Year >= 1400).ToList();
                    Assert.Equal(3,res.Count);

                    res = session.Query<PersonAndDate>().Where(x => x.Date.Day < 7).ToList();
                    Assert.Equal(4, res.Count);

                    res = session.Query<PersonAndDate>().Where(x => x.Date.Month > 7 && x.Date.Month <= 11).ToList();
                    Assert.Equal(1, res.Count);

                    res = session.Query<PersonAndDate>().Where(x => x.Date.Hour >= 20 ).ToList();
                    Assert.Equal(1, res.Count);

                    res = session.Query<PersonAndDate>().Where(x => x.Date.Minute <= 20).ToList();
                    Assert.Equal(3, res.Count);

                    res = session.Query<PersonAndDate>().Where(x => x.Date.Second == 43).ToList();
                    Assert.Equal(1, res.Count);

                    //1902-04-30​T10:40:00.000Z = 600000000000000000L
                    res = session.Query<PersonAndDate>().Where(x => x.Date.Ticks > 600000000000000000L).ToList();
                    Assert.Equal(1, res.Count);

                    res = session.Advanced.RawQuery<PersonAndDate>("from PersonAndDates where StartsWith(Date,'1234-05-06')").ToList();
                    Assert.Equal(1, res.Count);
                }
            }
        }

        [Fact]
        public void StaticQueryingDateTimeShouldWork()
        {
            using (var store = SetupStoreAndPushSomeEntities())
            {
                var index = new PersonByDate();
                store.ExecuteIndex(index);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Year >= 1400).ToList();
                    Assert.Equal(3, res.Count);

                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Day < 7).ToList();
                    Assert.Equal(4, res.Count);

                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Month > 7 && x.Date.Month <= 11).ToList();
                    Assert.Equal(1, res.Count);

                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Hour >= 20).ToList();
                    Assert.Equal(1, res.Count);

                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Minute <= 20).ToList();
                    Assert.Equal(3, res.Count);

                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Second == 43).ToList();
                    Assert.Equal(1, res.Count);

                    //1902-04-30​T10:40:00.000Z = 600000000000000000L
                    res = session.Query<PersonAndDate>(index.IndexName).Where(x => x.Date.Ticks > 600000000000000000L).ToList();
                    Assert.Equal(1, res.Count);

                    /* I think this should work but it throws in the linq provider that it doesn't know how to deal with ToString
                    res = session.Query<PersonAndDate>(index.IndexName).Where(x=>x.Date.ToString().StartsWith("1234-05-06")).ToList();
                    Assert.Equal(1, res.Count);
                    */
                }
            }
        }

        public class PersonByDate : AbstractIndexCreationTask<PersonAndDate>
        {
            public PersonByDate()
            {
                Map = docs => from doc in docs
                    select new
                    {
                        Date_Year = doc.Date.Year,
                        Date_Month = doc.Date.Month,
                        Date_Day = doc.Date.Day,
                        Date_Hour = doc.Date.Hour,
                        Date_Minute = doc.Date.Minute,
                        Date_Second = doc.Date.Second,
                        Date_Ticks = doc.Date.Ticks,
                        doc.Date
                    };
            }
        }
        private DocumentStore SetupStoreAndPushSomeEntities()
        {
            var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {       
                session.Store(new PersonAndDate
                {
                    Name = "Oren",
                    Date= new DateTime(1234, 5, 6, 7, 8, 9)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Tal",
                    Date = new DateTime(1400, 11, 6, 3, 23, 43)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Maxim",
                    Date = new DateTime(1654, 7, 17, 11, 24, 51)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Michael",
                    Date = new DateTime(666, 12, 4, 7, 11, 45)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Iftah",
                    Date = new DateTime(1, 1, 1, 1, 1, 1)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Karmel",
                    Date = new DateTime(2025, 4, 28, 23, 28, 23)
                });

                session.Store(new PersonAndDate
                {
                    Name = "Grisha",
                    Date = new DateTime(11, 2, 17, 19, 23, 31)
                });
                session.SaveChanges();
            }
            return store;
        }

        public class PersonAndDate
        {
            public string Name { get; set; }
            public DateTime Date { get; set; }
        }

    }
}
