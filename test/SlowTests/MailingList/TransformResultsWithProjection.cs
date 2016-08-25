// -----------------------------------------------------------------------
//  <copyright file="TransformResultsWithProjection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class TransformResultsWithProjection : RavenTestBase
    {
        [Fact(Skip = "Missing feature: Spatial")]
        public void CanGetProjectedResultsWhenUsingTransformWithInMemory()
        {
            using (var store = GetDocumentStore())
                Test(store);
        }

        private void Test(IDocumentStore store)
        {
            using (IDocumentSession session = store.OpenSession())
            {
                var lead = new Lead
                {
                    Id = "leads/1",
                    Lat = 52.5223618,
                    Lng = -1.8355805
                };

                var invoice = new BaseInvoice
                {
                    Id = "invoices/1",
                    LeadId = "leads/1",
                };
                invoice.Calls.Add(new ServiceCall
                {
                    Id = 1,
                    AppointmentDate = DateTime.Now
                });
                invoice.Calls.Add(new ServiceCall
                {
                    Id = 2,
                    AppointmentDate = DateTime.Now
                });

                session.Store(lead);
                session.Store(invoice);

                session.SaveChanges();
            }

            new ServiceCalls_Index().Execute(store);
            new NearbyServiceCallTransformer().Execute(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<ServiceCalls_Index.Result, ServiceCalls_Index>()
                    .Customize(
                        x =>
                            x.WaitForNonStaleResults().SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(true))
                    .Customize(x => x.WithinRadiusOf("Coordinates", 20, 52.5158768,
                        longitude: -1.7306246)
                        .SortByDistance())
                    .Where(x => x.AppointmentDate != null)
                    .ProjectFromIndexFieldsInto<ServiceCalls_Index.Result>()
                    .TransformWith<NearbyServiceCallTransformer, NearbyServiceCallItemViewModel>()
                    .ToList();

                Assert.Equal(2, results.Count());
            }
        }

        private class BaseInvoice
        {
            public BaseInvoice()
            {
                Calls = new List<ServiceCall>();
            }

            public string Id { get; set; }

            public string LeadId { get; set; }

            public List<ServiceCall> Calls { get; set; }
        }

        private class Lead
        {
            public string Id { get; set; }
            public double Lat { get; set; }
            public double Lng { get; set; }
        }

        private class NearbyServiceCallItemViewModel
        {
            public string Id { get; set; }
            public string LeadId { get; set; }
            public string InvoiceId { get; set; }


            public double Lat { get; set; }
            public double Lng { get; set; }


            public DateTime AppointmentDate { get; set; }
        }

        private class NearbyServiceCallTransformer : AbstractTransformerCreationTask<ServiceCalls_Index.Result>
        {
            public NearbyServiceCallTransformer()
            {
                TransformResults = calls => from call in calls
                                            let sale = LoadDocument<BaseInvoice>(call.InvoiceId)
                                            let lead = LoadDocument<Lead>(sale.LeadId)
                                            select new
                                            {
                                                LeadId = lead.Id,
                                                call.InvoiceId,
                                                call.Id,
                                                call.AppointmentDate,
                                            };
            }
        }

        private class ServiceCall
        {
            public int Id { get; set; }
            public DateTime AppointmentDate { get; set; }
        }

        private class ServiceCalls_Index : AbstractIndexCreationTask<BaseInvoice, ServiceCalls_Index.Result>
        {
            public ServiceCalls_Index()
            {
                Map = docs => from doc in docs
                              from call in doc.Calls
                              let lead = LoadDocument<Lead>(doc.LeadId)
                              select new
                              {
                                  InvoiceId = doc.Id,
                                  call.Id,
                                  call.AppointmentDate,

                                  // so we can do projection + transform + spatial query together we need these in the index + stored
                                  lead.Lat,
                                  lead.Lng,
                                  __ = SpatialGenerate("Coordinates", lead.Lat, lead.Lng)
                              };

                StoreAllFields(FieldStorage.Yes);
            }

            public class Result
            {
                public string InvoiceId { get; set; }
                public string Id { get; set; }
                public DateTimeOffset? AppointmentDate { get; set; }

                public double GeoLatitude { get; set; }
                public double GeoLongitude { get; set; }
            }
        }
    }
}
