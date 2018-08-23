using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11795 : RavenTestBase
    {
        [Fact]
        public void SearchBooking_ProjectionWithDateTimeToStringAndFormat_ReturnsResult()
        {
            using (var store = GetDocumentStore())
            { 
                // Arrange  
                store.ExecuteIndex(new BookingIndex());

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(2));

                    session.Store(new Booking { FirstName = "Alex", LastName = "Me", Start = DateTime.Parse("2018-01-01T11:11:11") });
                    session.Store(new Booking { FirstName = "You", LastName = "Me", Start = DateTime.Parse("2017-11-11T10:10:10") });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                // Act
                using (var session = store.OpenSession())
                {
                    var query = session.Query<BookingIndex.Result, BookingIndex>()
                        .Where(x => x.FullName == "Alex Me")
                        .Select(x => new {
                            FullName = x.FullName,
                            StartDate = x.Start.ToString("dd.MM.yyyy")
                        });

                    Assert.Equal("from index 'BookingIndex' as x where x.FullName = $p0 " +
                                 "select { FullName : x.FullName, StartDate : toDateString(new Date(Date.parse(x.Start)), \"dd.MM.yyyy\") }"
                            , query.ToString());

                    var result = query.Single();

                    // Assert
                    Assert.NotNull(result);
                    Assert.Equal("01.01.2018", result.StartDate);
                }
            }
        }

        private class Booking
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime Start { get; set; }
        }

        private class BookingIndex : AbstractIndexCreationTask<Booking>
        {
            public class Result
            {
                public string Id { get; set; }
                public string FullName { get; set; }
                public DateTime Start { get; set; }
            }

            public BookingIndex()
            {
                Map = bookings => from booking in bookings
                                  select new Result
                                  {
                                      Id = booking.Id,
                                      FullName = booking.FirstName + " " + booking.LastName,
                                      Start = booking.Start,
                                  };

                Store("FullName", FieldStorage.Yes);
            }
        }

    }
}
