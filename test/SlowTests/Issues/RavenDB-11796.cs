using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11796 : RavenTestBase
    {
        [Fact]
        public void ToStringOnNonStoredMissingFieldShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                // Arrange  
                store.ExecuteIndex(new BookingIndexFullNameIsFullNameMapStartToBegin());
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(2));
                    session.Store(new Booking { FirstName = "Alex", LastName = "Me", Start = DateTime.Parse("2018-01-01T11:11:11") });
                    session.Store(new Booking { FirstName = "You", LastName = "Me", Start = DateTime.Parse("2017-11-11T10:10:10") });
                    session.SaveChanges();
                }
                // Act
                using (var session = store.OpenSession())
                {
                    var result = session.Query<BookingIndexFullNameIsFullNameMapStartToBegin.Result, BookingIndexFullNameIsFullNameMapStartToBegin>()
                        .Where(x => x.FullName == "Alex Me")
                        .Select(x => new {
                            Start = x.Begin.ToString(),
                        }).Single();
                    // Assert
                    Assert.NotNull(result);
                    Assert.Null(result.Start);
                }
            }
        }

        private class BookingIndexFullNameIsFullNameMapStartToBegin : AbstractIndexCreationTask<Booking>
        {
            public class Result
            {
                public string Id { get; set; }
                public string FullName { get; set; }
                public DateTime Begin { get; set; }
            }
            public BookingIndexFullNameIsFullNameMapStartToBegin()
            {
                Map = bookings => from booking in bookings
                                  select new Result
                                  {
                                      Id = booking.Id,
                                      FullName = booking.FirstName + " " + booking.LastName,
                                      Begin = booking.Start,
                                  };
            }
        }
        private class Booking
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime Start { get; set; }
        }
    }
}
