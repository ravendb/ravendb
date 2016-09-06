// -----------------------------------------------------------------------
//  <copyright file="IndexCompilation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexCompilation : RavenTestBase
    {
        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanCompileIndex()
        {
            using (var store = GetDocumentStore())
            {
                new AccommodationFlightGeoNodePriceCalendarIndex().Execute(store);
            }
        }

        private class AccommodationFlightGeoNodePriceCalendarIndex : AbstractIndexCreationTask<AccommodationFlightPriceCalendarGeoNode>
        {

            public AccommodationFlightGeoNodePriceCalendarIndex()
            {
                Map = priceCalendarGeoNodes => from priceCalendarGeoNode in priceCalendarGeoNodes
                                               from period in priceCalendarGeoNode.Periods
                                               from date in period.Dates
                                               from flight in date.Flights
                                               where date.Accommodation != null && date.Accommodation.AccommodationArrivalDate.HasValue
                                               && date.Flights.Any() && priceCalendarGeoNode.Periods.Any()
                                               select new
                                               {
                                                   Year = priceCalendarGeoNode.Year,
                                                   Month = priceCalendarGeoNode.Month,
                                                   GeoNodeId = priceCalendarGeoNode.GeoNodeId,
                                                   Date = date.Date,
                                                   PersonConfiguration = priceCalendarGeoNode.PersonConfiguration,
                                                   PeriodDefinition = period.Period.StartDaysMask + "-" + period.Period.StayLength,
                                                   OutboundFromLocationId = flight.OutboundDepartureLocationId,
                                                   Price = flight.FlightPriceFrom.Value + date.Accommodation.AccommodationPriceFrom.Value,
                                                   AccommodationPriceExpiresAt = date.Accommodation.AccommodationArrivalDate,
                                                   FlightPriceExpiresAt = flight.PriceExpiresAt
                                               };
            }

        }

        private class AccommodationFlightPriceCalendarAccommodationPrice
        {
            public DateTime? AccommodationArrivalDate { get; private set; }
            public decimal? AccommodationPriceFrom { get; private set; }
        }

        private class AccommodationFlightPriceCalendarFlightPrice
        {
            public int? OutboundDepartureLocationId { get; private set; }
            public decimal? FlightPriceFrom { get; private set; }
            public DateTime PriceExpiresAt { get; private set; }
        }

        private class AccommodationFlightPriceCalendarGeoNode
        {
            public string Id { get; private set; }
            public int GeoNodeId { get; private set; }
            public List<PricedPeriodDefinition> Periods { get; private set; }
            public int Month { get; private set; }
            public int Year { get; private set; }
            public string PersonConfiguration { get; private set; }
        }

        private class PeriodDefinition
        {
            public int StartDaysMask { get; private set; }
            public int StayLength { get; private set; }
            public string Description { get; set; }
        }

        private class PricedDated
        {
            public DateTime Date { get; private set; }
            public AccommodationFlightPriceCalendarAccommodationPrice Accommodation { get; set; }
            public HashSet<AccommodationFlightPriceCalendarFlightPrice> Flights { get; set; }
        }

        private class PricedPeriodDefinition
        {
            public PeriodDefinition Period { get; private set; }
            public List<PricedDated> Dates { get; private set; }
        }
    }
}
