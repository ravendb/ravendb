using System;
using System.Linq;
using System.Collections.ObjectModel;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList.spokeypokey
{
    public class spokeypokey2 : RavenTestBase
    {
        public spokeypokey2(ITestOutputHelper output) : base(output)
        {
        }

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

        private class DummyIndex : AbstractMultiMapIndexCreationTask
        {
            public class IndexEntry
            {
                public DateTime? EffectiveFrom { get; set; }   
                public DateTime? EffectiveThrough { get; set; }  
                public string Zip { get; set; }
                public AddressTypeEnumPto2 AddressTypePto { get; set; }
                public DateTime? ContactEffectiveFrom { get; set; }
                public DateTime? ContactEffectiveThrough { get; set; }
                public DateTime? AddressEffectiveFrom { get; set; }
                public DateTime? AddressEffectiveThrough { get; set; }
            }
            
            public DummyIndex()
            {
                AddMap<ProviderPto2>(providers => from provider in providers
                    from providerAddressAssignmentPto in provider.AddressesPto
                    select new IndexEntry()
                    {
                        EffectiveFrom = providerAddressAssignmentPto.EffectiveFrom, 
                        EffectiveThrough = providerAddressAssignmentPto.EffectiveThrough,
                        Zip = providerAddressAssignmentPto.Address.Zip,
                        AddressTypePto = providerAddressAssignmentPto.Address.AddressTypePto,
                        ContactEffectiveFrom = null,
                        ContactEffectiveThrough = null,
                        AddressEffectiveFrom = null,
                        AddressEffectiveThrough = null
                    });
                
                AddMap<ProviderPto2>(providers => from provider in providers
                    from practiceOfficeAssignmentPto in provider.PracticeOfficesPto
                    select new IndexEntry()
                    {
                        EffectiveFrom = practiceOfficeAssignmentPto.EffectiveFrom,
                        EffectiveThrough = practiceOfficeAssignmentPto.EffectiveThrough,
                        Zip = practiceOfficeAssignmentPto.PrimaryContact.Address.Zip,
                        AddressTypePto = practiceOfficeAssignmentPto.PrimaryContact.Address.AddressTypePto,
                        ContactEffectiveFrom = practiceOfficeAssignmentPto.PrimaryContact.ContactEffectiveFrom,
                        ContactEffectiveThrough = practiceOfficeAssignmentPto.PrimaryContact.ContactEffectiveThrough,
                        AddressEffectiveFrom = practiceOfficeAssignmentPto.PrimaryContact.AddressEffectiveFrom,
                        AddressEffectiveThrough = practiceOfficeAssignmentPto.PrimaryContact.AddressEffectiveThrough
                    });
                
                AddMap<ProviderPto2>(providers => from provider in providers
                    from payToPto in provider.PayTosPto
                    select new IndexEntry()
                    {
                        EffectiveFrom = null,
                        EffectiveThrough = null,
                        Zip = payToPto.PrimaryContact.Zip,
                        AddressTypePto = payToPto.PrimaryContact.AddressTypePto,
                        ContactEffectiveFrom = null,
                        ContactEffectiveThrough = null,
                        AddressEffectiveFrom = null,
                        AddressEffectiveThrough = null
                    });
            }
        }

        private void CreateTestData(IDocumentStore DocStore)
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

                var index = new DummyIndex();
                
                index.Execute(DocStore);
                
                Indexes.WaitForIndexing(DocStore);
            }
        }

        //1105376
        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_search_by_Zip(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                    var query = from indexEntry in session.Query<DummyIndex.IndexEntry, DummyIndex>()
                        where
                            // Search for Provider Addresses: Zip
                            (indexEntry.Zip == searchCriteria.Zip
                             && indexEntry.AddressTypePto.In(searchCriteria.AddressTypes))

                            // ... or PracticeOffice Addresses : Zip
                            || (indexEntry.Zip == searchCriteria.Zip
                                &&
                                indexEntry.AddressTypePto.In(
                                    searchCriteria.AddressTypes) &&
                                // Check PracticeOffice effective dates
                                (indexEntry.EffectiveFrom <= fromCutoffDate)
                                &&
                                ((indexEntry.EffectiveThrough == null) ||
                                 (indexEntry.EffectiveThrough >= thruCutoffDate)) &&
                                // Check Contact effective dates
                                (indexEntry.ContactEffectiveFrom <= fromCutoffDate) &&
                                ((indexEntry.ContactEffectiveThrough == null) ||
                                 (indexEntry.ContactEffectiveThrough >= thruCutoffDate)) &&
                                // Check Contact address effective dates
                                (indexEntry.AddressEffectiveFrom <= fromCutoffDate) &&
                                ((indexEntry.AddressEffectiveThrough == null) ||
                                 (indexEntry.AddressEffectiveThrough >= thruCutoffDate))
                            )

                            // ... or Vendor Addresses: Zip
                            || (indexEntry.Zip == searchCriteria.Zip
                                && indexEntry.AddressTypePto.In(searchCriteria.AddressTypes))
                        select indexEntry;

                    Assert.Equal(1, query.ToArray().Length);
                }
            }
        }
    }
}
