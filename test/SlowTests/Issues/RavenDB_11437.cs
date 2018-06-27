using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11437 : RavenTestBase
    {
        [Fact]
        public void Can_index_complex_id_field()
        {
            using (var store = GetDocumentStore())
            {
                new Booking_ByConsultantId().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Booking()
                    {
                        Consultant = new Consultant()
                        {
                            Id = new IdField()
                            {
                                Id = Guid.Parse("21e6b989-0b4f-42b3-959a-a90b00b19b85")
                            }
                        }
                    });

                    session.SaveChanges();

                    var results = session.Query<Booking, Booking_ByConsultantId>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class Booking_ByConsultantId : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition()
                {
                    Maps =
                    {
                        @"docs.Bookings.Select(booking => new {
    ConsultantId = booking.Consultant.Id.Id
})"
                    }
                };
            }
        }

        private class EmployeeNumber
        {
            public int Number { get; set; }
        }

        private class IdField
        {
            public Guid Id { get; set; }
        }

        private class Consultant
        {
            public EmployeeNumber EmployeeNumber { get; set; }
            public IdField Id { get; set; }
        }

        private class Booking
        {
            public DateTimeOffset BookingDate { get; set; }
            public Consultant Consultant { get; set; }
            public string Description { get; set; }
            public string Id { get; set; }
        }
    }
}
