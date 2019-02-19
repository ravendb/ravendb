// -----------------------------------------------------------------------
//  <copyright file="LongIds.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class LongIds : RavenTestBase
    {
        [Fact]
        public void Embedded()
        {
            using (var store = GetDocumentStore())
            {
                var customer = new TestCustomer
                {
                    Id = "LoremipsumdolorsitametconsecteturadipiscingelitPraesentlobortisconguecursusCurabiturconvallisnuncmattisliberomolestieidiaculismagnaimperdietDuisnecenimsednislvestibulumvulputateDonecnuncarcumolestieeutinciduntacfermentumpretiumestAeneannoncondimentumorciDonecsitametanteerossedgravidaestQuisqueturpismaurisplaceratsedaliquamidgravidasednislIntegermetusleoultriciesegetiaculisnonporttitornonlacusProinegetfringillalectusCrasfeugiatloremaauctoregestasmienimpulvinarsemquisbibendumloremvelitnonnullaDonecultriciesfe"
                };

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(customer);
                    session.SaveChanges();
                }

                // This works
                using (var session = store.OpenSession())
                {
                    IEnumerable<TestCustomer> actual = session.Query<TestCustomer>().Customize(x => x.WaitForNonStaleResults())
                        .ToArray();
                    Assert.Equal(customer.Id, actual.Single().Id);
                }

                // This fails with invalid operation exception 
                using (IDocumentSession session = store.OpenSession())
                {
                    var loadedCustomer = session.Load<TestCustomer>(customer.Id);
                    Assert.NotNull(loadedCustomer);
                }
            }
        }

        [Fact]
        public void Remote()
        {
            using (var store = GetDocumentStore())
            {
                var customer = new TestCustomer
                {
                    Id = "LoremipsumdolorsitametconsecteturadipiscingelitPraesentlobortisconguecursusCurabiturconvallisnuncmattisliberomolestieidiaculismagnaimperdietDuisnecenimsednislvestibulumvulputateDonecnuncarcumolestieeutinciduntacfermentumpretiumestAeneannoncondimentumorciDonecsitametanteerossedgravidaestQuisqueturpismaurisplaceratsedaliquamidgravidasednislIntegermetusleoultriciesegetiaculisnonporttitornonlacusProinegetfringillalectusCrasfeugiatloremaauctoregestasmienimpulvinarsemquisbibendumloremvelitnonnullaDonecultriciesfe"
                };

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(customer);
                    session.SaveChanges();
                }

                // This works
                using (var session = store.OpenSession())
                {
                    IEnumerable<TestCustomer> actual = session.Query<TestCustomer>()
                        .Customize(x => x.WaitForNonStaleResults()).ToArray();
                    Assert.Equal(customer.Id, actual.Single().Id);
                }

                // This fails with invalid operation exception 
                using (IDocumentSession session = store.OpenSession())
                {
                    var loadedCustomer = session.Load<TestCustomer>(customer.Id);
                    Assert.NotNull(loadedCustomer);
                }
            }
        }

        private class TestCustomer
        {
            public string Id { get; set; }
        }
    }
}
