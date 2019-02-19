using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11513 : RavenTestBase
    {
        [Fact]
        public void ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);
            }
        }

        private class UserIndex : AbstractIndexCreationTask<MyUser>
        {
            public UserIndex()
            {
                this.Map = users => from user in users
                                    let partners = user.Partners == null ? null : this.LoadDocument<Partner>(user.Partners)
                                    let customers = user.Customers == null ? null : this.LoadDocument<Customer>(user.Customers)
                                    let sites = user.Sites == null ? null : this.LoadDocument<Site>(user.Sites)

                                    let partnerParentCarriers = partners == null ? new List<string>() : partners.Select(x => x.CarrierId).ToList()
                                    let customerParentCarriers = customers == null ? new List<string>() : customers.Select(x => x.CarrierId).ToList()
                                    let siteParentCarriers = sites == null ? new List<string>() : sites.Select(x => x.CarrierId).ToList()
                                    let actualCarriers = user.Carriers == null ? new List<string>() : user.Carriers

                                    let customerParentPartners = customers == null ? new List<string>() : customers.Select(x => x.PartnerId).ToList()
                                    let siteParentPartners = sites == null ? new List<string>() : sites.Select(x => x.PartnerId).ToList()
                                    let actualPartners = user.Partners == null ? new List<string>() : user.Partners

                                    let siteParentCustomers = sites == null ? new List<string>() : sites.Select(x => x.CustomerId).ToList()
                                    let actualCustomers = user.Customers == null ? new List<string>() : user.Customers
                                    select new
                                    {
                                        user.Id,
                                        user.FirstName,
                                        user.LastName,
                                        user.Email,
                                        user.IsActive,
                                        user.Customers,
                                        user.Partners,
                                        user.Carriers,
                                        user.Sites,
                                        user.Type,
                                        user.CreatorId,
                                        user.ResetToken,
                                        Searchable = $"{user.Email} {user.FirstName} {user.LastName}",
                                        ParentCarriers = partnerParentCarriers.Union(customerParentCarriers).Union(siteParentCarriers).Union(actualCarriers),
                                        ParentPartners = customerParentPartners.Union(siteParentPartners).Union(actualPartners),
                                        ParentCustomers = siteParentCustomers.Union(actualCustomers),
                                    };
            }
        }

        private class MyUser
        {
            public List<string> Partners { get; set; }

            public List<string> Customers { get; set; }

            public List<string> Sites { get; set; }

            public List<string> Carriers { get; set; }
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Email { get; set; }
            public bool IsActive { get; set; }
            public string Type { get; set; }
            public string CreatorId { get; set; }
            public string ResetToken { get; set; }
        }

        private class Partner
        {
            public string CarrierId { get; set; }
        }

        private class Customer
        {
            public string CarrierId { get; set; }
            public string PartnerId { get; set; }
        }

        private class Site
        {
            public string CarrierId { get; set; }
            public string PartnerId { get; set; }
            public string CustomerId { get; set; }
        }
    }
}
