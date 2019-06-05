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

        [Fact]
        public void CanCompileIndexWithToDictionary()
        {
            const int countOfUsers = 20;
            var listOfUsers = new List<User>();
            using (var store = GetDocumentStore())
            {
                new ToDictionarySelectOrderBySumIndex().Execute(store);
                var rnd = new System.Random();
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfUsers; i++)
                    {
                        var u = new User
                        {
                            Id = $"{i}",
                            LoginCountByDate = new Dictionary<DateTime, int>
                            {
                                {DateTime.UtcNow, rnd.Next(1, 100)},
                                {DateTime.UtcNow.AddDays(1), rnd.Next(1, 100)},
                                {DateTime.UtcNow.AddDays(-1), rnd.Next(1, 100)}
                            },
                            ListOfDecimals = new List<decimal>()
                            {
                                rnd.Next(1, 10),
                                rnd.Next(20, 30),
                                rnd.Next(40, 50),
                                rnd.Next(60, 70)
                            }
                        };

                        listOfUsers.Add(u);
                        session.Store(u);
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ToDictionarySelectOrderBySumIndex.Result, ToDictionarySelectOrderBySumIndex>()
                        .ProjectInto<ToDictionarySelectOrderBySumIndex.Result>()
                        .ToList();
                    Assert.Equal(countOfUsers, results.Count);
                    for (int i = 0; i < countOfUsers; i++)
                    {
                        var expectedResult = new ToDictionarySelectOrderBySumIndex.Result()
                        {
                            Id = listOfUsers[i].Id,
                            SelectSum = listOfUsers[i].LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).OrderBy(x => x.Value).Select(x => x.Value).Sum(x => x),
                            OrderBySum = listOfUsers[i].LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).Select(x => x.Value).Sum(x => x),
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.SelectSum, results[i].SelectSum);
                        Assert.Equal(expectedResult.OrderBySum, results[i].OrderBySum);
                    }
                }
            }
        }

        [Fact]
        public void CanCompileIndexWithDistinct()
        {
            const int countOfUsers = 20;
            var listOfUsers = new List<User>();
            using (var store = GetDocumentStore())
            {
                new DistinctSelectOrderBySumMapReduceIndex().Execute(store);
                var rnd = new System.Random();

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfUsers; i++)
                    {
                        var u = new User
                        {
                            Id = $"{i}",
                            LoginCountByDate = new Dictionary<DateTime, int>
                            {
                                {DateTime.UtcNow, rnd.Next(1, 100)},
                                {DateTime.UtcNow.AddDays(1), rnd.Next(1, 100)},
                                {DateTime.UtcNow.AddDays(-1), rnd.Next(1, 100)}
                            },
                            ListOfDecimals = new List<decimal>()
                            {
                                rnd.Next(1, 10),
                                rnd.Next(20, 30),
                                rnd.Next(40, 50),
                                rnd.Next(60, 70)
                            }
                        };
                        listOfUsers.Add(u);
                        session.Store(u);
                    }

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DistinctSelectOrderBySumMapReduceIndex.Result, DistinctSelectOrderBySumMapReduceIndex>()
                        .ProjectInto<DistinctSelectOrderBySumMapReduceIndex.Result>()
                        .ToList();

                    Assert.Equal(countOfUsers, results.Count);

                    for (int i = 0; i < countOfUsers; i++)
                    {
                        var expectedResult = new DistinctSelectOrderBySumMapReduceIndex.Result()
                        {
                            Id = listOfUsers[i].Id,
                            SelectSum = listOfUsers[i].LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).OrderBy(x => x.Value).Select(x => x.Value).Sum(x => x),
                            OrderBySum = listOfUsers[i].LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).Select(x => x.Value).Sum(x => x),
                            IdsWithDecimals = listOfUsers[i].ListOfDecimals.ToDictionary(k => k, k => 0),
                            Items = new[] { listOfUsers[i].Id }
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.SelectSum, results[i].SelectSum);
                        Assert.Equal(expectedResult.OrderBySum, results[i].OrderBySum);
                        Assert.Equal(expectedResult.Items, results[i].Items);
                    }
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
                GroupJoin = order.Lines.GroupJoin(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => y).Count(),
                TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault(),
                SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault(),
                LongCount = order.Lines.LongCount(),
                LongCountWithPredicate = order.Lines.LongCount(x => x.Quantity > 7)
            };

            Assert.Equal(expectedResult.SmallestQuantity, first.SmallestQuantity);
            Assert.Equal(expectedResult.LargestQuantity, first.LargestQuantity);

            Assert.Equal(expectedResult.Aggregate, first.Aggregate);
            Assert.Equal(expectedResult.AggregateWithSeed, first.AggregateWithSeed);
            Assert.Equal(expectedResult.AggregateWithSeedAndSelector, first.AggregateWithSeedAndSelector);

            Assert.Equal(expectedResult.Join, first.Join);
            Assert.Equal(expectedResult.GroupJoin, first.GroupJoin);

            Assert.Equal(expectedResult.TakeWhile, first.TakeWhile);
            Assert.Equal(expectedResult.TakeWhileIndexWithIndex, first.TakeWhileIndexWithIndex);
            Assert.Equal(expectedResult.SkipWhile, first.SkipWhile);
            Assert.Equal(expectedResult.SkipWhileIndexWithIndex, first.SkipWhileIndexWithIndex);

            Assert.Equal(expectedResult.LongCount, first.LongCount);
            Assert.Equal(expectedResult.LongCountWithPredicate, first.LongCountWithPredicate);
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

                public int GroupJoin { get; set; }

                public int TakeWhile { get; set; }

                public int TakeWhileIndexWithIndex { get; set; }

                public int SkipWhile { get; set; }

                public int SkipWhileIndexWithIndex { get; set; }

                public long LongCount { get; set; }

                public long LongCountWithPredicate { get; set; }
            }

            public TypedThenByIndex()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    SmallestQuantity = order.Lines.OrderBy(x => x.Discount).ThenBy(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                                    LargestQuantity = order.Lines.OrderBy(x => x.Discount).ThenByDescending(x => x.Quantity).Select(x => x.Quantity).FirstOrDefault(),
                                    Aggregate = order.Lines.Select(x => x.Quantity).Aggregate((q1, q2) => q1 + q2),
                                    AggregateWithSeed = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2),
                                    AggregateWithSeedAndSelector = order.Lines.Select(x => x.Quantity).Aggregate(13, (q1, q2) => q1 + q2, x => x + 500),
                                    Join = order.Lines.Join(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => x).Count(),
                                    GroupJoin = order.Lines.GroupJoin(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => y).Count(),
                                    TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                                    TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault(),
                                    SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count(),
                                    SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault(),
                                    ToLookup = order.Lines.ToLookup(x => x.Quantity),
                                    ToLookupWithElementSelector = order.Lines.ToLookup(x => x.Quantity, o => o.Discount),
                                    LongCount = order.Lines.LongCount(),
                                    LongCountWithPredicate = order.Lines.LongCount(x => x.Quantity > 7)
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
                        "GroupJoin = order.Lines.GroupJoin(order.Lines, x => x.Quantity, y => y.Quantity, (x, y) => y).Count()," +
                        "TakeWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count()," +
                        "TakeWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 1).Select(x => x.Quantity).FirstOrDefault()," +
                        "SkipWhile = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile(x => x.Quantity > 10).Count()," +
                        "SkipWhileIndexWithIndex = order.Lines.OrderByDescending(x => x.Quantity).TakeWhile((x, c) => x.Quantity > 10 && c == 0).Select(x => x.Quantity).FirstOrDefault()," +
                        "ToLookup = order.Lines.ToLookup(x => x.Quantity)," +
                        "ToLookupWithElementSelector = order.Lines.ToLookup(x => x.Quantity, o => o.Discount)," +
                        "LongCount = order.Lines.LongCount()," +
                        "LongCountWithPredicate = order.Lines.LongCount(x => x.Quantity > 7)" +
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
                        {nameof(TypedThenByIndex.Result.GroupJoin), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.TakeWhile), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.TakeWhileIndexWithIndex), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.SkipWhile), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.SkipWhileIndexWithIndex), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.LongCount), new IndexFieldOptions {Storage = FieldStorage.Yes}},
                        {nameof(TypedThenByIndex.Result.LongCountWithPredicate), new IndexFieldOptions {Storage = FieldStorage.Yes}}
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

        private class ToDictionarySelectOrderBySumIndex : AbstractIndexCreationTask<User, ToDictionarySelectOrderBySumIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }

                public int SelectSum { get; set; }

                public int OrderBySum { get; set; }

                public Dictionary<decimal, int> IdsWithDecimals { get; set; }
            }

            public ToDictionarySelectOrderBySumIndex()
            {
                Map = users => from user in users
                    select new Result
                    {
                        Id = user.Id,
                        SelectSum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).OrderBy(x => x.Value).Select(x => x.Value).Sum(x => x),
                        OrderBySum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).Select(x => x.Value).Sum(x => x),
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class DistinctSelectOrderBySumMapReduceIndex : AbstractIndexCreationTask<User, DistinctSelectOrderBySumMapReduceIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }

                public int SelectSum { get; set; }

                public int OrderBySum { get; set; }

                public Dictionary<decimal, int> IdsWithDecimals { get; set; }

                public IList<string> Items { get; set; } = new List<string>();
            }

            public DistinctSelectOrderBySumMapReduceIndex()
            {
                Map = users => from user in users
                               select new Result
                               {
                                   Id = user.Id,
                                   SelectSum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).OrderBy(x => x.Value).Select(x => x.Value).Sum(x => x),
                                   OrderBySum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).Select(x => x.Value).Sum(x => x),
                                   IdsWithDecimals = user.ListOfDecimals.ToDictionary(i => i, i => 0),
                                   Items = new[] { user.Id }
                               };

                Reduce = results => from result in results
                    group result by new
                    {
                        result.Id,
                        result.OrderBySum,
                        result.SelectSum
                    }
                    into g
                    let numbersDictionary = g.SelectMany(x => x.IdsWithDecimals).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                    select new Result
                    {
                        Id = g.Key.Id,
                        SelectSum = g.Key.SelectSum,
                        OrderBySum = g.Key.OrderBySum,
                        IdsWithDecimals = numbersDictionary,
                        Items = g.SelectMany(x => x.Items).Distinct().OrderBy(x => x).ToList()
                    };
            }
        }

        public class User
        {
            public string Id { get; set; }
            public Dictionary<DateTime, int> LoginCountByDate { get; set; }
            public List<decimal> ListOfDecimals { get; set; }
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
