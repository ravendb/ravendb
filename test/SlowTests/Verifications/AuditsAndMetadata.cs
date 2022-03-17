using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Verifications
{
    public class AuditsAndMetadata : RavenTestBase
    {
        public AuditsAndMetadata(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// The only thing i could not recreate here is the fact that the revisions is active.
        /// So im not sure if the @last-modified meta-field will ever be filled in this case
        /// </summary>
        [Fact]
        public void CreateDataAndQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.OnBeforeStore += (sender, args) =>
                    {
                        args.DocumentMetadata["Updated-By"] = "DataImporter";
                    };


                    session.Store(new Customer
                    {
                        Name = "Test",
                        CreatedBy = "Me",
                        CreatedOn = DateTimeOffset.Now,
                        CustomerType = CustomerType.Customer,
                        InternalId = 1234
                    });

                    //what should happen here is that the revisions would do its stuff
                    //and the AuditListener pushes the "Updated-By" key into the metadata
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Advanced.RawQuery<CustomerListingModel>(@"
from Customers as customer
select {
    Id: id(customer),
    InternalId: customer.InternalId,
    Name: customer.Name,
    Type: customer.CustomerType,
    CreatedOn: customer.CreatedOn,
    ChangedBy: customer['@metadata']['Updated-By'],
}")
                                        .FirstOrDefault();
                    Assert.Equal(customer.ChangedBy, "DataImporter");
                }
            }
        }
        private class Customer : AuditableEntity
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public long InternalId { get; set; }

            public CustomerType CustomerType { get; set; }

            public DateTimeOffset CreatedOn { get; set; }
            public string CreatedBy { get; set; }
        }

        /// <summary>
        /// Specifies the type of a customer
        /// </summary>
        private enum CustomerType
        {
            Prospect,
            Customer,
            Partner
        }

        private class CustomerListingModel
        {
            public string Id { get; set; }
            public long InternalId { get; set; }

            public string Name { get; set; }
            public string Type { get; set; }
            public DateTime CreatedOn { get; set; }
            public DateTime? ChangedOn { get; set; } //modified this to a nullable to illustrate the issue better
            public string ChangedBy { get; set; }
        }

        private class AuditableEntity
        {

        }

    }
}
