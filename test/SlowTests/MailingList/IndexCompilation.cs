// -----------------------------------------------------------------------
//  <copyright file="IndexCompilation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class IndexCompilation : RavenTestBase
    {
        public IndexCompilation(ITestOutputHelper output) : base(output)
        {
        }

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
                            DateTimeIntDictionary = listOfUsers[i].LoginCountByDate.ToDictionary(y => y.Key, y => y.Value)
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.SelectSum, results[i].SelectSum);
                        Assert.Equal(expectedResult.OrderBySum, results[i].OrderBySum);
                        Assert.Equal(expectedResult.DateTimeIntDictionary, results[i].DateTimeIntDictionary);
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
                            OrderByDescending = listOfUsers[i].LoginCountByDate.OrderByDescending(x => x.Value).ToDictionary(y => y.Key.GetDefaultRavenFormat(isUtc: y.Key.Kind == DateTimeKind.Utc), y => y.Value),
                            Items = new[] { listOfUsers[i].Id }
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.SelectSum, results[i].SelectSum);
                        Assert.Equal(expectedResult.OrderBySum, results[i].OrderBySum);
                        Assert.Equal(expectedResult.OrderByDescending, results[i].OrderByDescending);
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

                public Dictionary<DateTime, int> DateTimeIntDictionary { get; set; }
            }

            public ToDictionarySelectOrderBySumIndex()
            {
                Map = users => from user in users
                               select new Result
                               {
                                   Id = user.Id,
                                   SelectSum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).OrderBy(x => x.Value).Select(x => x.Value).Sum(x => x),
                                   OrderBySum = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value).Select(x => x.Value).Sum(x => x),
                                   DateTimeIntDictionary = user.LoginCountByDate.ToDictionary(y => y.Key, y => y.Value)
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

                public Dictionary<string, int> OrderByDescending { get; set; }

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
                                   OrderByDescending = user.LoginCountByDate.OrderByDescending(x => x.Value).ToDictionary(y => y.Key.ToString(CultureInfo.InvariantCulture), y => y.Value),
                                   Items = new[] { user.Id }
                               };

                Reduce = results => from result in results
                                    group result by new
                                    {
                                        result.Id,
                                        result.OrderBySum,
                                        result.SelectSum,
                                        result.OrderByDescending
                                    }
                    into g
                                    let numbersDictionary = g.SelectMany(x => x.IdsWithDecimals).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    select new Result
                                    {
                                        Id = g.Key.Id,
                                        SelectSum = g.Key.SelectSum,
                                        OrderBySum = g.Key.OrderBySum,
                                        IdsWithDecimals = numbersDictionary,
                                        OrderByDescending = g.Key.OrderByDescending,
                                        Items = g.SelectMany(x => x.Items).Distinct().OrderBy(x => x).ToList()
                                    };
            }
        }


        [Fact]
        public void DynamicDictionaryIndexShouldWork()
        {
            const int countOfEmployees = 20;
            var listOfUsers = new List<Employee>();
            using (var store = GetDocumentStore())
            {
                new DynamicDictionaryTestMapReduceIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var u = new Employee
                        {
                            Id = $"{i}",
                            LoginCountByDate = new Dictionary<DateTime, int>
                            {
                                {new DateTime(), 44},
                                {new DateTime().AddDays(1), 55},
                                {new DateTime().AddDays(10), 66}
                            },
                            DictionaryOfIntegers = new Dictionary<int, int>
                            {
                                {1, 77},
                                {2, 22},
                                {5, 33}
                            },
                            DictionaryOfIntegers2 = new Dictionary<int, int>
                            {
                                {2, 44},
                                {1, 55},
                                {4, 66}
                            },
                            ListOfDecimals = new List<decimal>()
                            {
                               12,
                                38,
                                44,
                                66
                            },
                            ListOfDecimals2 = new List<decimal>()
                            {
                                33,
                                222,
                                444,
                               12,
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
                    var results = session.Query<DynamicDictionaryTestMapReduceIndex.Result, DynamicDictionaryTestMapReduceIndex>()
                        .ProjectInto<DynamicDictionaryTestMapReduceIndex.Result>()
                        .ToList();

                    Assert.Equal(countOfEmployees, results.Count);

                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var dict1 = listOfUsers[i].ListOfDecimals.ToDictionary(x => x, x => 5).ToDictionary(x => x.Key, x => x.Value);
                        var dict2 = listOfUsers[i].ListOfDecimals2.ToDictionary(x => x, x => 2);
                        var intDict1 = listOfUsers[i].DictionaryOfIntegers2;
                        var intDict2 = listOfUsers[i].DictionaryOfIntegers;
                        var expectedResult = new DynamicDictionaryTestMapReduceIndex.Result()
                        {
                            Id = listOfUsers[i].Id,
                            DictionarySum = listOfUsers[i].DictionaryOfIntegers.ToDictionary(y => y.Key, y => y.Value).Sum(x => x.Value),
                            DictionarySumAggregate = listOfUsers[i].DictionaryOfIntegers.ToDictionary(y => y.Key, y => y.Value).Aggregate(0, (x1, x2) => x1 + x2.Value),
                            OrderByDescending = listOfUsers[i].LoginCountByDate.OrderByDescending(x => x.Value).ToDictionary(y => y.Key.GetDefaultRavenFormat(isUtc: y.Key.Kind == DateTimeKind.Utc), y => y.Value),
                            IntIntDic = intDict1,
                            IntIntDic2 = intDict2,
                            IdsWithDecimals = dict1,
                            IdsWithDecimals2 = dict2,
                            RemainingFt = dict1.Join(dict2, tot => tot.Key, good => good.Key, (tot, good) => Math.Max(tot.Value - (int)good.Value, 0)).Aggregate(0m, (d1, d2) => d1 + d2),
                            CompleteFt = dict2.Select(y => y.Value).Aggregate(0m, (d1, d2) => d1 + d2),
                            TotalFt = dict1.Select(y => y.Value).Aggregate(0m, (d1, d2) => d1 + d2),
                            RemainingQty = intDict1.Join(intDict2, q1 => q1.Key, q2 => q2.Key, (q, qdone) => Math.Max(q.Value - (int)qdone.Value, 0)).Aggregate(0, (i1, i2) => i1 + i2),
                            ScheduleState = State.Done,
                            Items = new List<string>
                            {
                                listOfUsers[i].Id
                            },
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.DictionarySum, results[i].DictionarySum);
                        Assert.Equal(expectedResult.DictionarySumAggregate, results[i].DictionarySumAggregate);
                        Assert.Equal(expectedResult.OrderByDescending, results[i].OrderByDescending);
                        Assert.Equal(expectedResult.IntIntDic, results[i].IntIntDic2);
                        Assert.Equal(expectedResult.IntIntDic2, results[i].IntIntDic);
                        Assert.Equal(expectedResult.RemainingFt, results[i].RemainingFt);
                        Assert.Equal(expectedResult.CompleteFt, results[i].CompleteFt);
                        Assert.Equal(expectedResult.TotalFt, results[i].TotalFt);
                        Assert.Equal(expectedResult.RemainingQty, results[i].RemainingQty);
                        Assert.Equal(expectedResult.ScheduleState, results[i].ScheduleState);
                        Assert.Equal(expectedResult.Items, results[i].Items);
                    }
                }
            }
        }

        [Fact]
        public void DynamicDictionaryIndexShouldWorkWithCount()
        {
            const int countOfEmployees = 20;
            var listOfUsers = new List<Employee>();
            using (var store = GetDocumentStore())
            {
                new DynamicDictionaryTestMapIndexWithCount().Execute(store);
                var rnd = new System.Random();
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var u = new Employee
                        {
                            Id = $"{i}",
                            DictionaryOfIntegers = new Dictionary<int, int>
                            {
                                {1, 1},
                                {rnd.Next(11, 20), rnd.Next(1, 100)},
                                {rnd.Next(21, 30), rnd.Next(1, 100)}
                            },
                            DictionaryOfStringInteger = new Dictionary<string, int>
                            {
                                { "int", 322 }
                            },
                            DictionaryOfStringShort = new Dictionary<string, short>
                            {
                                { "short", 322 }
                            },
                            DictionaryOfStringLong = new Dictionary<string, long>
                            {
                                { "long", 322 }
                            },
                            DictionaryOfStringDouble = new Dictionary<string, double>
                            {
                                { "double", 3.22 }
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
                    var results = session.Query<DynamicDictionaryTestMapIndexWithCount.Result, DynamicDictionaryTestMapIndexWithCount>()
                        .ProjectInto<DynamicDictionaryTestMapIndexWithCount.Result>()
                        .ToList();

                    Assert.Equal(countOfEmployees, results.Count);

                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var dict = listOfUsers[i].DictionaryOfIntegers;
                        var strIntDict = listOfUsers[i].DictionaryOfStringInteger;
                        var strDoubleDict = listOfUsers[i].DictionaryOfStringDouble;
                        var expectedResult = new DynamicDictionaryTestMapIndexWithCount.Result()
                        {
                            Count = listOfUsers[i].DictionaryOfIntegers.Count,
                            Count2 = dict.Count,
                            ContainsIntInt = dict.Contains(new KeyValuePair<int, int>(1, 1)),
                            ContainsInt = strIntDict.Contains(new KeyValuePair<string, int>("int", 322)),
                            ContainsDouble = strDoubleDict.Contains(new KeyValuePair<string, double>("double", 3.22)),
                            ContainsShort = listOfUsers[i].DictionaryOfStringShort.Contains(new KeyValuePair<string, short>("short", 322)),
                            ContainsLong = listOfUsers[i].DictionaryOfStringLong.Contains(new KeyValuePair<string, long>("long", 322))
                        };
                        Assert.Equal(expectedResult.Count, results[i].Count);
                        Assert.Equal(expectedResult.Count2, results[i].Count2);
                        Assert.Equal(results[i].Count, results[i].Count2);
                        Assert.Equal(expectedResult.ContainsIntInt, results[i].ContainsIntInt);
                        Assert.Equal(expectedResult.ContainsInt, results[i].ContainsInt);
                        Assert.Equal(expectedResult.ContainsDouble, results[i].ContainsDouble);
                        Assert.Equal(expectedResult.ContainsShort, results[i].ContainsShort);
                        Assert.Equal(expectedResult.ContainsLong, results[i].ContainsLong);
                    }
                }
            }
        }
        private class DynamicDictionaryTestMapIndexWithCount : AbstractIndexCreationTask<Employee, DynamicDictionaryTestMapIndexWithCount.Result>
        {
            public class Result
            {
                public int Count { get; set; }
                public int Count2 { get; set; }
                public bool ContainsIntInt { get; set; }
                public bool ContainsInt { get; set; }
                public bool ContainsDouble { get; set; }
                public bool ContainsShort { get; set; }
                public bool ContainsLong { get; set; }
            }

            public DynamicDictionaryTestMapIndexWithCount()
            {
                Map = employees => from e in employees
                                   let dict = e.DictionaryOfIntegers.ToDictionary(x => x.Key, x => x.Value)
                                   select new Result
                                   {
                                       Count = e.DictionaryOfIntegers.ToDictionary(x => x.Key, x => x.Value).Count(),
                                       Count2 = dict.Count(),
                                       ContainsIntInt = dict.Contains(new KeyValuePair<int, int>(1, 1)),
                                       ContainsInt = e.DictionaryOfStringInteger.Contains(new KeyValuePair<string, int>("int", 322)),
                                       ContainsShort = e.DictionaryOfStringShort.ToDictionary(x => x.Key, x => x.Value).Contains(new KeyValuePair<string, short>("short", 322)),
                                       ContainsLong = e.DictionaryOfStringLong.Contains(new KeyValuePair<string, long>("long", 322)),
                                       ContainsDouble = e.DictionaryOfStringDouble.ToDictionary(x => x.Key, x => x.Value).Contains(new KeyValuePair<string, double>("double", 3.22)),
                                   };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void DynamicDictionaryIndexShouldWorkWithExtensionMethods()
        {
            const int countOfEmployees = 20;
            var listOfUsers = new List<Employee>();
            using (var store = GetDocumentStore())
            {
                new DynamicDictionaryTestMapIndexWithExtensionMethods().Execute(store);
                var rnd = new System.Random();
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var u = new Employee
                        {
                            Id = $"{i}",
                            DictionaryOfIntegers = new Dictionary<int, int>
                            {
                                {rnd.Next(1, 10), rnd.Next(1, 100)},
                                {rnd.Next(11, 20), rnd.Next(1, 100)},
                                {rnd.Next(21, 30), rnd.Next(1, 100)}
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
                    var results = session.Query<DynamicDictionaryTestMapIndexWithExtensionMethods.Result, DynamicDictionaryTestMapIndexWithExtensionMethods>()
                        .ProjectInto<DynamicDictionaryTestMapIndexWithExtensionMethods.Result>()
                        .ToList();

                    Assert.Equal(countOfEmployees, results.Count);

                    for (int i = 0; i < countOfEmployees; i++)
                    {
                        var dict = listOfUsers[i].DictionaryOfIntegers.GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value));
                        var expectedResult = new DynamicDictionaryTestMapIndexWithExtensionMethods.Result()
                        {
                            Id = listOfUsers[i].Id,
                            DictionaryAggregateOne = dict.Aggregate(0, (x1, x2) => x1 + x2.Value),
                            DictionarySumOne = listOfUsers[i].DictionaryOfIntegers.GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value)).Sum(x => x.Value),
                            DictionaryAggregateTwo = listOfUsers[i].DictionaryOfIntegers.ToDictionary(y => y.Key, y => y.Value).Aggregate(0, (x1, x2) => x1 + x2.Value),
                            DictionarySumTwo = listOfUsers[i].DictionaryOfIntegers.GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value)).Sum(x => x.Value),
#pragma warning disable CS0183 // 'is' expression's given expression is always of the provided type
                            IsDictionaryOfInt = listOfUsers[i].DictionaryOfIntegers.All(x => (int)x.Value is int),
#pragma warning restore CS0183 // 'is' expression's given expression is always of the provided type
                            LongCount = listOfUsers[i].DictionaryOfIntegers.LongCount(pair => pair.Value > 50)
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.DictionaryAggregateOne, results[i].DictionaryAggregateOne);
                        Assert.Equal(expectedResult.DictionarySumOne, results[i].DictionarySumOne);
                        Assert.Equal(expectedResult.DictionaryAggregateTwo, results[i].DictionaryAggregateTwo);
                        Assert.Equal(expectedResult.DictionarySumTwo, results[i].DictionarySumTwo);
                        Assert.Equal(expectedResult.IsDictionaryOfInt, results[i].IsDictionaryOfInt);
                        Assert.Equal(expectedResult.LongCount, results[i].LongCount);
                        Assert.Equal(null, results[i].DictionaryOfIntegers);
                    }
                }
            }
        }

        private class DynamicDictionaryTestMapIndexWithExtensionMethods : AbstractIndexCreationTask<Employee, DynamicDictionaryTestMapIndexWithExtensionMethods.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public int DictionaryAggregateOne { get; set; }
                public int DictionarySumOne { get; set; }
                public Dictionary<int, int> DictionaryOfIntegers { get; set; }
                public int DictionaryAggregateTwo { get; set; }
                public int DictionarySumTwo { get; set; }
                public bool IsDictionaryOfInt { get; set; }
                public long LongCount { get; set; }
            }

            public DynamicDictionaryTestMapIndexWithExtensionMethods()
            {
                Map = employees => from e in employees
                                   select new Result
                                   {
                                       Id = e.Id,
                                       DictionaryAggregateOne = 0,
                                       DictionarySumOne = 0,
                                       DictionaryAggregateTwo = 0,
                                       DictionarySumTwo = 0,
                                       DictionaryOfIntegers = e.DictionaryOfIntegers,
                                       IsDictionaryOfInt = false,
                                       LongCount = 0L
                                   };
                Reduce = results => from
                        result in results
                                    group result by new
                                    {
                                        result.Id
                                    }
                    into g
                                    let dic = g.SelectMany(x => x.DictionaryOfIntegers).ToDictionary(y => y.Key, y => y.Value)
                                    let dicDic = g.SelectMany(x => x.DictionaryOfIntegers).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    let dicTotalAggregate = dic.Aggregate(0, (x1, x2) => x1 + x2.Value)
                                    let dicTotalSum = dic.Sum(x => x.Value)
                                    select new Result
                                    {
                                        Id = g.Key.Id,
                                        DictionaryAggregateTwo = dicTotalAggregate,
                                        DictionarySumTwo = dicTotalSum,
                                        DictionaryAggregateOne = dicDic.Aggregate(0, (x1, x2) => x1 + x2.Value),
                                        DictionarySumOne = dicDic.Sum(x => x.Value),
                                        DictionaryOfIntegers = null,
#pragma warning disable CS0183 // 'is' expression's given expression is always of the provided type
                                        IsDictionaryOfInt = dic.All(x => (int)x.Value is int),
#pragma warning restore CS0183 // 'is' expression's given expression is always of the provided type
                                        LongCount = dic.LongCount(pair => pair.Value > 50)
                                    };
            }
        }

        private class DynamicDictionaryTestMapReduceIndex : AbstractIndexCreationTask<Employee, DynamicDictionaryTestMapReduceIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public int DictionarySum { get; set; }
                public int DictionarySumAggregate { get; set; }
                public Dictionary<decimal, int> IdsWithDecimals { get; set; }
                public Dictionary<decimal, int> IdsWithDecimals2 { get; set; }
                public Dictionary<string, int> OrderByDescending { get; set; }
                public Dictionary<int, int> IntIntDic { get; set; }
                public IList<string> Items { get; set; } = new List<string>();
                public Dictionary<int, int> IntIntDic2 { get; set; }
                public decimal CompleteFt { get; set; }
                public decimal RemainingFt { get; set; }
                public decimal TotalFt { get; set; }
                public int RemainingQty { get; set; }
                public State ScheduleState { get; set; }
            }

            public DynamicDictionaryTestMapReduceIndex()
            {
                Map = employees => from e in employees
                                   select new Result
                                   {
                                       Id = e.Id,
                                       DictionarySum = e.DictionaryOfIntegers.ToDictionary(y => y.Key, y => y.Value).Sum(x => x.Value),
                                       DictionarySumAggregate = e.DictionaryOfIntegers.ToDictionary(y => y.Key, y => y.Value).Aggregate(0, (x1, x2) => x1 + x2.Value),
                                       IdsWithDecimals = e.ListOfDecimals2.ToDictionary(i => i, i => 5),
                                       IdsWithDecimals2 = e.ListOfDecimals.ToDictionary(i => i, i => 2),
                                       OrderByDescending = e.LoginCountByDate.OrderByDescending(x => x.Value).ToDictionary(y => y.Key.ToString(CultureInfo.InvariantCulture), y => y.Value),
                                       IntIntDic = e.DictionaryOfIntegers,
                                       IntIntDic2 = e.DictionaryOfIntegers2,
                                       Items = new[] { e.Id },
                                       CompleteFt = 0,
                                       RemainingFt = 0,
                                       TotalFt = 0,
                                       RemainingQty = 0,
                                       ScheduleState = IndexCompilation.State.Undone
                                   };

                Reduce = results => from result in results
                                    group result by new
                                    {
                                        result.Id,
                                        result.OrderByDescending,
                                        result.DictionarySum,
                                        result.DictionarySumAggregate
                                    }
                    into g
                                    let dic1 = g.SelectMany(x => x.IdsWithDecimals).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    let dic2 = g.SelectMany(x => x.IdsWithDecimals2).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    let dic3 = g.SelectMany(x => x.IntIntDic2).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    let dic4 = g.SelectMany(x => x.IntIntDic).ToDictionary(y => y.Key, y => y.Value)
                                    let totalQty = dic4.Aggregate((int)0, (x1, x2) => (int)x1 + (int)x2.Value)
                                    let remainingQty = dic4.Join(dic3, q1 => q1.Key, q2 => q2.Key, (q, qdone) => Math.Max(q.Value - (int)qdone.Value, 0)).Aggregate(0, (i1, i2) => i1 + i2)
                                    let numbersDictionary = g.SelectMany(x => x.IdsWithDecimals).GroupBy(x => x.Key).ToDictionary(y => y.Key, y => y.Sum(x => x.Value))
                                    let totalFt = g.SelectMany(x => x.IdsWithDecimals.Select(y => y.Value)).Aggregate(0m, (d1, d2) => d1 + d2)
                                    let completeFt = g.SelectMany(x => x.IdsWithDecimals2.Select(y => y.Value)).Aggregate(0m, (d1, d2) => d1 + d2)
                                    let remainingFt = dic1.Join(dic2, tot => tot.Key, good => good.Key, (tot, good) => Math.Max(tot.Value - (int)good.Value, 0)).Aggregate(0m, (d1, d2) => d1 + d2)
                                    let scheduleState = totalQty <= 0 ? IndexCompilation.State.Undone : IndexCompilation.State.Done
                                    select new Result
                                    {
                                        Id = g.Key.Id,
                                        DictionarySum = g.Key.DictionarySum,
                                        DictionarySumAggregate = g.Key.DictionarySumAggregate,
                                        IdsWithDecimals = dic2,
                                        IdsWithDecimals2 = dic1,
                                        IntIntDic = dic4,
                                        IntIntDic2 = dic3,
                                        OrderByDescending = g.Key.OrderByDescending,
                                        Items = g.SelectMany(x => x.Items).Distinct().OrderBy(x => x).ToList(),
                                        CompleteFt = completeFt,
                                        TotalFt = totalFt,
                                        RemainingFt = remainingFt,
                                        RemainingQty = remainingQty,
                                        ScheduleState = scheduleState
                                    };
            }
        }

        [Fact]
        public void DynamicDictionaryIndexShouldWorkWithMethods()
        {
            const int countOfVisitors = 20;
            var listOfVisitors = new List<Visitor>();
            using (var store = GetDocumentStore())
            {
                new DynamicDictionaryIndex().Execute(store);
                var rnd = new System.Random();
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < countOfVisitors; i++)
                    {
                        var u = new Visitor
                        {
                            Id = $"{i}",
                            DictionaryOfIntegers = new Dictionary<int, int>
                            {
                                {1, rnd.Next(1, 100)},
                                {rnd.Next(4, 10), rnd.Next(1, 100)},
                                {rnd.Next(11, 20), rnd.Next(1, 100)},
                                {rnd.Next(21, 30), rnd.Next(1, 100)},
                                {322, 322}
                            },
                            DictionaryOfIntegersToUnion = new Dictionary<int, int>
                            {
                                {2, rnd.Next(1, 100)},
                                {3, rnd.Next(1, 100)},
                            },
                            DictionaryOfIntegersToIntersect = new Dictionary<int, int>
                            {
                                {1, rnd.Next(1, 100)},
                                {322, 322},
                            },
                            DictionaryOfIntegersToExcept = new Dictionary<int, int>
                            {
                                {322, 322},
                            }
                        };
                        listOfVisitors.Add(u);
                        session.Store(u);
                    }

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DynamicDictionaryIndex.Result, DynamicDictionaryIndex>()
                        .ProjectInto<DynamicDictionaryIndex.Result>()
                        .ToList();

                    Assert.Equal(countOfVisitors, results.Count);
                    for (int i = 0; i < countOfVisitors; i++)
                    {
                        var expectedResult = new DynamicDictionaryIndex.Result()
                        {
                            Id = listOfVisitors[i].Id,
                            ContainsKeyResult = listOfVisitors[i].DictionaryOfIntegers.ContainsKey(1),
                            LastResult = listOfVisitors[i].DictionaryOfIntegers.Last(),
                            LastOrDefaultResult = listOfVisitors[i].DictionaryOfIntegers.LastOrDefault(),
                            ElementAtResult = listOfVisitors[i].DictionaryOfIntegers.ElementAt(0),
                            ElementAtOrDefaultResult = listOfVisitors[i].DictionaryOfIntegers.ElementAtOrDefault(0),
                            AnyResult = listOfVisitors[i].DictionaryOfIntegers.Any(),
                            AnyWithPredicateResult = listOfVisitors[i].DictionaryOfIntegers.Any(x => x.Value > 1),

                            SkipResult = listOfVisitors[i].DictionaryOfIntegers.Skip(1).ToDictionary(x => x.Key, x => x.Value),
                            SkipLastResult = listOfVisitors[i].DictionaryOfIntegers.SkipLast(1).ToDictionary(x => x.Key, x => x.Value),
                            TakeResult = listOfVisitors[i].DictionaryOfIntegers.Take(3).ToDictionary(x => x.Key, x => x.Value),
                            TakeLastResult = listOfVisitors[i].DictionaryOfIntegers.TakeLast(3).ToDictionary(x => x.Key, x => x.Value),
                            UnionResult = listOfVisitors[i].DictionaryOfIntegers.Union(listOfVisitors[i].DictionaryOfIntegersToUnion).ToDictionary(x => x.Key, x => x.Value),
                            IntersectResult = listOfVisitors[i].DictionaryOfIntegers.Intersect(listOfVisitors[i].DictionaryOfIntegersToIntersect).ToDictionary(x => x.Key, x => x.Value),
                            PrependResult = listOfVisitors[i].DictionaryOfIntegers.Prepend(new KeyValuePair<int, int>(321, 322)).ToDictionary(x => x.Key, x => x.Value),
                            ExceptResult = listOfVisitors[i].DictionaryOfIntegers.Except(listOfVisitors[i].DictionaryOfIntegersToExcept).ToDictionary(x => x.Key, x => x.Value),

                            //TryGetValueResult = listOfVisitors[i].DictionaryOfIntegers.TryGetValue(1, out _),
                        };
                        Assert.Equal(expectedResult.Id, results[i].Id);
                        Assert.Equal(expectedResult.ContainsKeyResult, results[i].ContainsKeyResult);
                        Assert.Equal(expectedResult.LastResult, results[i].LastResult);
                        Assert.Equal(expectedResult.LastOrDefaultResult, results[i].LastOrDefaultResult);
                        Assert.Equal(expectedResult.LastResult, results[i].LastOrDefaultResult); // Last converted to LastOrDefault in index expression
                        Assert.Equal(expectedResult.ElementAtResult, results[i].ElementAtResult);
                        Assert.Equal(expectedResult.ElementAtOrDefaultResult, results[i].ElementAtOrDefaultResult);
                        Assert.Equal(expectedResult.AnyResult, results[i].AnyResult);
                        Assert.Equal(expectedResult.AnyWithPredicateResult, results[i].AnyWithPredicateResult);

                        Assert.Equal(expectedResult.SkipResult, results[i].SkipResult);
                        Assert.Equal(expectedResult.SkipLastResult, results[i].SkipLastResult);
                        Assert.Equal(expectedResult.TakeResult, results[i].TakeResult);
                        Assert.Equal(expectedResult.TakeLastResult, results[i].TakeLastResult);
                        Assert.Equal(expectedResult.UnionResult, results[i].UnionResult);
                        Assert.Equal(expectedResult.IntersectResult, results[i].IntersectResult);
                        Assert.Equal(expectedResult.PrependResult, results[i].PrependResult);
                        Assert.Equal(expectedResult.ExceptResult, results[i].ExceptResult);

                        //Assert.Equal(expectedResult.TryGetValueResult, results[i].TryGetValueResult);
                    }
                }
            }
        }

        private class DynamicDictionaryIndex : AbstractIndexCreationTask<Visitor, DynamicDictionaryIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public bool ContainsKeyResult { get; set; }
                public bool AnyResult { get; set; }
                public bool AnyWithPredicateResult { get; set; }
                public KeyValuePair<int, int> LastResult { get; set; }
                public KeyValuePair<int, int> LastOrDefaultResult { get; set; }
                public KeyValuePair<int, int> ElementAtResult { get; set; }
                public KeyValuePair<int, int> ElementAtOrDefaultResult { get; set; }
                public Dictionary<int, int> SkipResult { get; set; }
                public Dictionary<int, int> SkipLastResult { get; set; }
                public Dictionary<int, int> TakeResult { get; set; }
                public Dictionary<int, int> TakeLastResult { get; set; }
                public Dictionary<int, int> UnionResult { get; set; }
                public Dictionary<int, int> IntersectResult { get; set; }
                public Dictionary<int, int> PrependResult { get; set; }
                public Dictionary<int, int> ExceptResult { get; set; }

                public bool TryGetValueResult { get; set; }
            }
            public DynamicDictionaryIndex()
            {
                Map = visitors => from e in visitors
                                  select new Result
                                  {
                                      Id = e.Id,
                                      ContainsKeyResult = e.DictionaryOfIntegers.ContainsKey(1),
                                      LastResult = e.DictionaryOfIntegers.Last(),
                                      LastOrDefaultResult = e.DictionaryOfIntegers.LastOrDefault(),
                                      ElementAtResult = e.DictionaryOfIntegers.ElementAt(0),
                                      ElementAtOrDefaultResult = e.DictionaryOfIntegers.ElementAtOrDefault(0),
                                      AnyResult = e.DictionaryOfIntegers.Any(),
                                      AnyWithPredicateResult = e.DictionaryOfIntegers.Any(x => x.Value > 1),

                                      SkipResult = e.DictionaryOfIntegers.Skip(1).ToDictionary(x=>x.Key, x=>x.Value),
                                      SkipLastResult = e.DictionaryOfIntegers.SkipLast(1).ToDictionary(x => x.Key, x => x.Value),
                                      TakeResult = e.DictionaryOfIntegers.Take(3).ToDictionary(x => x.Key, x => x.Value),
                                      TakeLastResult = e.DictionaryOfIntegers.TakeLast(3).ToDictionary(x => x.Key, x => x.Value),
                                      UnionResult = e.DictionaryOfIntegers.Union(e.DictionaryOfIntegersToUnion).ToDictionary(x => x.Key, x => x.Value),
                                      IntersectResult = e.DictionaryOfIntegers.Intersect(e.DictionaryOfIntegersToIntersect).ToDictionary(x => x.Key, x => x.Value),
                                      PrependResult = e.DictionaryOfIntegers.Prepend(new KeyValuePair<int, int>(321, 322)).ToDictionary(x => x.Key, x => x.Value),
                                      ExceptResult = e.DictionaryOfIntegers.Except(e.DictionaryOfIntegersToExcept).ToDictionary(x => x.Key, x => x.Value),

                                      //TryGetValueResult = e.DictionaryOfIntegers.TryGetValue(1, out s),
                                  };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class Visitor
        {
            public string Id { get; set; }
            public Dictionary<int, int> DictionaryOfIntegers { get; set; }
            public Dictionary<int, int> DictionaryOfIntegersToUnion { get; set; }
            public Dictionary<int, int> DictionaryOfIntegersToIntersect { get; set; }
            public Dictionary<int, int> DictionaryOfIntegersToExcept { get; set; }
        }

        public class User
        {
            public string Id { get; set; }
            public Dictionary<DateTime, int> LoginCountByDate { get; set; }
            public List<decimal> ListOfDecimals { get; set; }
        }

        public class Employee
        {
            public string Id { get; set; }
            public Dictionary<DateTime, int> LoginCountByDate { get; set; }
            public List<decimal> ListOfDecimals { get; set; }
            public List<decimal> ListOfDecimals2 { get; set; }
            public Dictionary<int, int> DictionaryOfIntegers { get; set; }
            public Dictionary<int, int> DictionaryOfIntegers2 { get; set; }
            public Dictionary<string, int> DictionaryOfStringInteger { get; set; }
            public Dictionary<string, double> DictionaryOfStringDouble { get; set; }
            public Dictionary<string, short> DictionaryOfStringShort { get; set; }
            public Dictionary<string, long> DictionaryOfStringLong { get; set; }
            public Dictionary<string, float> DictionaryOfStringFloat { get; set; }
            public Dictionary<string, decimal> DictionaryOfStringDecimal { get; set; }
            public Dictionary<string, byte> DictionaryOfStringByte { get; set; }
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

        private enum State
        {
            Done,
            Undone
        }
    }
}
