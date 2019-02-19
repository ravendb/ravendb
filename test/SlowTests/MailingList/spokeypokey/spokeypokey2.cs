using System;
using System.Linq;
using System.Collections.ObjectModel;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList.spokeypokey
{
    public class spokeypokey2 : RavenTestBase
    {
        private enum AddressTypeEnumPto2
        {
            ProviderAddress = 0,
            PracticeOfficeAddress = 1,
            PayToAddress = 2,
        }

        private class AddressSummaryPto2
        {
            public string Zip { get; set; }
            public AddressTypeEnumPto2 AddressTypePto { get; set; }
        }

        private class PracticeOfficeContactReferencePto2
        {
            public AddressSummaryPto2 Address { get; set; }
            public DateTime ContactEffectiveFrom { get; set; }
            public DateTime? ContactEffectiveThrough { get; set; }
            public DateTime AddressEffectiveFrom { get; set; }
            public DateTime? AddressEffectiveThrough { get; set; }
        }


        private class PracticeOfficeAssignmentPto2
        {
            public DateTime EffectiveFrom { get; set; }
            public DateTime? EffectiveThrough { get; set; }
            public PracticeOfficeContactReferencePto2 PrimaryContact { get; set; }
        }

        private class ProviderAddressAssignmentPto2
        {
            public AddressSummaryPto2 Address { get; set; }
            public DateTime EffectiveFrom { get; set; }
            public DateTime? EffectiveThrough { get; set; }
        }

        private class PayToAssignmentPto2
        {
            public DateTime EffectiveFrom { get; set; }
            public DateTime? EffectiveThrough { get; set; }
            public AddressSummaryPto2 PrimaryContact { get; set; }
        }

        private class ProviderPto2
        {
            public Collection<ProviderAddressAssignmentPto2> AddressesPto { get; set; }
            public Collection<PracticeOfficeAssignmentPto2> PracticeOfficesPto { get; set; }
            public Collection<PayToAssignmentPto2> PayTosPto { get; set; }
        }

        private class SearchProviderParamsPto
        {
            public string Zip { get; set; }
            public Collection<AddressTypeEnumPto2> AddressTypes { get; set; }
        }


        private static void CreateTestData(IDocumentStore DocStore)
        {
            var provider1 = new ProviderPto2();
            provider1.AddressesPto =
                new Collection<ProviderAddressAssignmentPto2>
                    {
                        new ProviderAddressAssignmentPto2
                            {
                                Address = new AddressSummaryPto2
                                            {
                                                Zip = "WA000",
                                                AddressTypePto = AddressTypeEnumPto2.ProviderAddress
                                            },
                                EffectiveFrom = new DateTime(2011, 1, 1),
                                EffectiveThrough = new DateTime(2011, 2, 1),
                            }
                    };

            // Practice Office Addresses
            provider1.PracticeOfficesPto =
                new Collection<PracticeOfficeAssignmentPto2>
                    {
                        new PracticeOfficeAssignmentPto2
                            {
                                PrimaryContact = new PracticeOfficeContactReferencePto2
                                                    {
                                                        Address = new AddressSummaryPto2
                                                                    {
                                                                        Zip = "ID000",
                                                                        AddressTypePto = AddressTypeEnumPto2.PracticeOfficeAddress
                                                                    },
                                                        ContactEffectiveFrom = new DateTime(2011, 1, 1),
                                                        ContactEffectiveThrough = new DateTime(2011, 2, 1),
                                                        AddressEffectiveFrom = new DateTime(2011, 1, 1),
                                                        AddressEffectiveThrough = new DateTime(2011, 2, 1),
                                                    }
                            }
                    };

            // PayTo Addresses
            provider1.PayTosPto =
                new Collection<PayToAssignmentPto2>
                    {
                        new PayToAssignmentPto2
                            {
                                PrimaryContact = new AddressSummaryPto2
                                                    {
                                                        Zip = "CA000",
                                                        AddressTypePto = AddressTypeEnumPto2.PayToAddress
                                                    },
                                EffectiveFrom = new DateTime(2011, 1, 1),
                                EffectiveThrough = new DateTime(2011, 2, 1),

                            }
                    };

            using (var session = DocStore.OpenSession())
            {
                session.Store(provider1);
                session.SaveChanges();
            }
        }

        //1105376
        [Fact]
        public void Can_search_by_Zip()
        {
            using (var store = GetDocumentStore())
            {
                CreateTestData(store);

                var fromCutoffDate = DateTime.MaxValue;
                var thruCutoffDate = DateTime.MinValue;
                var searchCriteria = new SearchProviderParamsPto
                {
                    Zip = "WA000",
                    AddressTypes = new Collection<AddressTypeEnumPto2>
                                                            {
                                                                AddressTypeEnumPto2.PayToAddress,
                                                                AddressTypeEnumPto2.PracticeOfficeAddress,
                                                                AddressTypeEnumPto2.ProviderAddress

                                                            }
                };

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<ProviderPto2>()
                                where
                                    // Search for Provider Addresses: Zip
                                    (p.AddressesPto.Any(x => x.Address.Zip == searchCriteria.Zip
                                                             && x.Address.AddressTypePto.In(searchCriteria.AddressTypes)))

                                    // ... or PracticeOffice Addresses : Zip
                                    || (p.PracticeOfficesPto.Any(x => x.PrimaryContact.Address.Zip == searchCriteria.Zip
                                                                      &&
                                                                      x.PrimaryContact.Address.AddressTypePto.In(
                                                                        searchCriteria.AddressTypes) &&
                                                                      // Check PracticeOffice effective dates
                                                                      (x.EffectiveFrom <= fromCutoffDate)
                                                                      &&
                                                                      ((x.EffectiveThrough == null) ||
                                                                       (x.EffectiveThrough >= thruCutoffDate)) &&
                                                                      // Check Contact effective dates
                                                                      (x.PrimaryContact.ContactEffectiveFrom <= fromCutoffDate) &&
                                                                      ((x.PrimaryContact.ContactEffectiveThrough == null) ||
                                                                       (x.PrimaryContact.ContactEffectiveThrough >= thruCutoffDate)) &&
                                                                      // Check Contact address effective dates
                                                                      (x.PrimaryContact.AddressEffectiveFrom <= fromCutoffDate) &&
                                                                      ((x.PrimaryContact.AddressEffectiveThrough == null) ||
                                                                       (x.PrimaryContact.AddressEffectiveThrough >= thruCutoffDate))
                                        ))

                                    // ... or Vendor Addresses: Zip
                                    || (p.PayTosPto.Any(x => x.PrimaryContact.Zip == searchCriteria.Zip
                                                             && x.PrimaryContact.AddressTypePto.In(searchCriteria.AddressTypes)))
                                select p;
                    Assert.Equal(1, query.ToArray().Length);
                }
            }
        }
    }
}
