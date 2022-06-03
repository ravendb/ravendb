using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class ComplexIndexes : RavenTestBase
    {
        public ComplexIndexes(ITestOutputHelper output) : base(output)
        {
        }


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCreateIndex(Options options)
        {
            using(var store = GetDocumentStore(options))
            {
                new ReadingHabitsByDayOfWeekMultiMap().Execute(store);
            }
        }

        private class ReadingHabitsByDayOfWeekMultiMap: AbstractMultiMapIndexCreationTask<ReadingHabitsByDayOfWeekMultiMap.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public CountPerDay[] CountsPerDay { get; set; }
                public string Name { get; set; }

                public class CountPerDay
                {
                    public DayOfWeek DayOfWeek { get; set; }
                    public int Count { get; set; }
                }
            }
            public ReadingHabitsByDayOfWeekMultiMap()
            {
                AddMap<ReadingList>(lists =>
                      from list in lists
                      select new
                      {
                          list.UserId,
                          Name = (string)null,
                          CountsPerDay = from b in list.Books
                                         group b by b.ReadAt.DayOfWeek into g
                                         select new
                                         {
                                             DayOfWeek = g.Key,
                                             Count = g.Count()
                                         }
                      });

                AddMap<User>(users =>
                             users.SelectMany(user => Enumerable.Range(0, 6), (user, day) => new
                             {
                                UserId = user.Id,
                                CountsPerDay = new object[0],
                                user.Name,
                             })
                    );

                Reduce = results =>
                         from result in results
                         group result by result.UserId
                             into g
                             select new
                             {
                                 UserId = g.Key,
                                 Name = g.Select(x => x.Name).FirstOrDefault(x => x != null),
                                 CountsPerDay = g.SelectMany(x => x.CountsPerDay).GroupBy(cpd => cpd.DayOfWeek).Select(gi => new
                                 {
                                     DayOfWeek = gi.Key,
                                     Count = gi.Sum(x => x.Count)
                                 })
                             };
            }
        }


        private class ReadingList
        {
            public string Id { get; set; }
            public string UserId { get; set; }

            public List<ReadBook> Books { get; set; }

            public class ReadBook
            {
                public string Title { get; set; }
                public DateTime ReadAt { get; set; }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
