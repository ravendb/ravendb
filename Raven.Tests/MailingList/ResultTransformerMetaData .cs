// -----------------------------------------------------------------------
//  <copyright file="ResultTransformerMetaData .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class ResultTransformerMetaData : RavenTest
    {
        /// <summary>
        /// The only thing i could not recreate here is the fact that the versioning bundle is active.
        /// So im not sure if the Last-Modified meta-field will ever be filled in this case
        /// </summary>
        [Fact]
        public void CreateDataAndQuery()
        {
            using (var store = NewDocumentStore())
            {

                store.RegisterListener(new AuditListener(() => "DataImporter"));
                new CustomerListingTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer
                    {
                        Name = "Test",
                        CreatedBy = "Me",
                        CreatedOn = DateTimeOffset.Now,
                        CustomerType = CustomerType.Customer,
                        InternalId = 1234
                    });

                    //what should happen here is that the versioning bundle would do its stuff
                    //and the AuditListener pushes the "Updated-By" key into the metadata
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Query<Customer>()
                                   .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                   .TransformWith<CustomerListingTransformer, CustomerListingModel>()
                                   .FirstOrDefault();

                    Assert.Equal(customer.ChangedBy, "DataImporter");
                    Assert.NotNull(customer.ChangedOn);
                }
            }
        }

        public class CustomerListingTransformer : AbstractTransformerCreationTask<Customer>
        {
            public CustomerListingTransformer()
            {
                TransformResults = customers => from customer in customers
                                                select new
                                                {
                                                    customer.Id,
                                                    customer.InternalId,
                                                    customer.Name,
                                                    Type = customer.CustomerType.ToString(),
                                                    CreatedOn = customer.CreatedOn.ToLocalTime(),
                                                    ChangedBy = this.MetadataFor(customer).Value<string>("Updated-By"),
                                                    ChangedOn = this.MetadataFor(customer).Value<DateTime?>("Last-Modified"),
                                                };

            }
        }

        public class Customer : AuditableEntity
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
        public enum CustomerType
        {
            Prospect,
            Customer,
            Partner
        }

        public class CustomerListingModel
        {
            public string Id { get; set; }
            public long InternalId { get; set; }

            public string Name { get; set; }
            public string Type { get; set; }
            public DateTime CreatedOn { get; set; }
            public DateTime? ChangedOn { get; set; } //modified this to a nullable to illustrate the issue better
            public string ChangedBy { get; set; }
        }

        public class AuditableEntity
        {

        }

        public class AuditListener : IDocumentStoreListener
        {
            private Func<string> authenticator;

            public AuditListener(Func<string> authenticator)
            {
                this.authenticator = authenticator;
            }

            public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
            {
                if (entityInstance is AuditableEntity)
                {
                    metadata["Updated-By"] = authenticator();
                    return true; //to indicate we changed something
                }
                return false;
            }

            public void AfterStore(string key, object entityInstance, RavenJObject metadata)
            {

            }
        }
    }
}