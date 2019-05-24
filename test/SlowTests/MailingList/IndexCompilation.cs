// -----------------------------------------------------------------------
//  <copyright file="IndexCompilation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexCompilation : RavenTestBase
    {
        [Fact]
        public void CanCompileIndex()
        {
            using (var store = GetDocumentStore())
            {
                new AccommodationFlightGeoNodePriceCalendarIndex().Execute(store);
            }
        }

        [Fact]
        public void CanCompileTypedIndexWithMethodExtensions()
        {
            using (var store = GetDocumentStore())
            {
                new TypedThenByIndex().Execute(store);

                var order = new Order
                {
                    Lines = new List<OrderLine>
                    {
                        new OrderLine {Quantity = 7, Discount = 4},
                        new OrderLine {Quantity = 50, Discount = 4},
                        new OrderLine {Quantity = 20, Discount = 4},
                        new OrderLine {Quantity = 3, Discount = 4},
                        new OrderLine {Quantity = 7, Discount = 4}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(order);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<TypedThenByIndex.Result, TypedThenByIndex>()
                        .ProjectInto<TypedThenByIndex.Result>()
                        .ToList();

                    AssertResults(result, order);
                }
            }
        }

        [Fact]
        public void CanCompileIndexWithMethodExtensions()
        {
            using (var store = GetDocumentStore())
            {
                new ThenByIndex().Execute(store);

                var order = new Order
                {
                    Lines = new List<OrderLine>
                    {
                        new OrderLine {Quantity = 7, Discount = 4},
                        new OrderLine {Quantity = 50, Discount = 4},
                        new OrderLine {Quantity = 20, Discount = 4},
                        new OrderLine {Quantity = 3, Discount = 4},
                        new OrderLine {Quantity = 7, Discount = 4}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(order);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<TypedThenByIndex.Result, ThenByIndex>()
                        .ProjectInto<TypedThenByIndex.Result>()
                        .ToList();

                    AssertResults(result, order);
                }
            }
        }

        private static void AssertResults(List<TypedThenByIndex.Result> result, Order order)
        {
            Assert.Equal(1, result.Count);

            var first = result.First();

            var expectedResult = new TypedThenByIndex.Result
            {
                SmallestQuantity = order.Lines.OrderBy(x => x.Discount).ThenBy(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                LargestQuantity = order.Lines.OrderBy(x => x.Discount).ThenByDescending(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                Aggregate = order.Lines.Select(x => x.Quantity).Aggregate((q1, q2) => q1 + q2),
                AggregateWithSeed = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2),
                AggregateWithSeedAndSelector = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2, x => x + 500),
                Join = order.Lines.Join(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => x).Count(),
                TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault(),
                SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault()
            };

            Assert.Equal(expectedResult.SmallestQuantity, first.SmallestQuantity);
            Assert.Equal(expectedResult.LargestQuantity, first.LargestQuantity);

            Assert.Equal(expectedResult.Aggregate, first.Aggregate);
            Assert.Equal(expectedResult.AggregateWithSeed, first.AggregateWithSeed);
            Assert.Equal(expectedResult.AggregateWithSeedAndSelector, first.AggregateWithSeedAndSelector);

            Assert.Equal(expectedResult.Join, first.Join);

            Assert.Equal(expectedResult.TakeWhile, first.TakeWhile);
            Assert.Equal(expectedResult.TakeWhileIndexWithIndex, first.TakeWhileIndexWithIndex);
            Assert.Equal(expectedResult.SkipWhile, first.SkipWhile);
            Assert.Equal(expectedResult.SkipWhileIndexWithIndex, first.SkipWhileIndexWithIndex);
        }

        public class TypedThenByIndex : AbstractIndexCreationTask<Order, TypedThenByIndex.Result>
        {
            public class Result
            {
                public int SmallestQuantity { get; set; }

                public int LargestQuantity { get; set; }

                public int Aggregate { get; set; }

                public int AggregateWithSeed { get; set; }

                public int AggregateWithSeedAndSelector { get; set; }

                public int Join { get; set; }

                public int TakeWhile { get; set; }

                public int TakeWhileIndexWithIndex { get; set; }

                public int SkipWhile { get; set; }

                public int SkipWhileIndexWithIndex { get; set; }
            }

            public TypedThenByIndex()
            {
                Map = orders => from order in orders
                                select new Result
                                {
                                    SmallestQuantity = order.Lines.OrderBy(x => x.Discount).ThenBy(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                                    LargestQuantity = order.Lines.OrderBy(x => x.Discount).ThenByDescending(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                                    Aggregate = order.Lines.Select(x => x.Quantity).Aggregate((q1, q2) => q1 + q2),
                                    AggregateWithSeed = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2),
                                    AggregateWithSeedAndSelector = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2, x => x + 500),
                                    Join = order.Lines.Join(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => x).Count(),
                                    TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                                    TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault(),
                                    SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                                    SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault()
                                };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class ThenByIndex : AbstractIndexCreationTask<Order, TypedThenByIndex.Result>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        "from order in docs.Orders select new { " +
                        "SmallestQuantity = order.Lines.OrderBy(x => x.Discount).ThenBy(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault()," +
                        "LargestQuantity = order.Lines.OrderBy(x => x.Discount).ThenByDescending(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault()," +
                        "Aggregate = order.Lines.Select(x => x.Quantity).Aggregate((q1, q2) => q1 + q2)," +
                        "AggregateWithSeed = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2)," +
                        "AggregateWithSeedAndSelector = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2, x => x + 500)," +
                        "Join = order.Lines.Join(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => x).Count()," +
                        "TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count()," +
                        "TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault()," +
                        "SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count()," +
                        "SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault()" +
                        "}"
                    },
                    Fields =
                    {
                        {nameof(TypedThenByIndex.Result.SmallestQuantity), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.LargestQuantity), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.Aggregate), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.AggregateWithSeed), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.AggregateWithSeedAndSelector), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.Join), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.TakeWhile), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.TakeWhileIndexWithIndex), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.SkipWhile), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.SkipWhileIndexWithIndex), new IndexFieldOptions {Storage = FieldStorage.Yes}}
                    }
                };
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
