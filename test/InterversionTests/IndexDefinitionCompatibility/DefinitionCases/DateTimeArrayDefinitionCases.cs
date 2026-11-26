using System;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateDateTimeArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/date-time/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountViaAgg = doc.ImportantDates.Aggregate(0, (acc, val) => acc + 1)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Earliest = doc.ImportantDates.Min(),
                                Latest = doc.ImportantDates.Max()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasFuture = doc.ImportantDates.Any(x => x.Year > 2050),
                                AllPast = doc.ImportantDates.All(x => x.Year < 2000),
                                Count20thCentury = doc.ImportantDates.Count(x => x.Year >= 1900 && x.Year < 2000)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalCount = doc.ImportantDates.Concat(new[] { new DateTime(2025, 1, 1) }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasDate = doc.ImportantDates.Contains(new DateTime(2022, 2, 24))
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.ImportantDates.DefaultIfEmpty(new DateTime(1900, 1, 1)).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.ImportantDates.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val0 = doc.ImportantDates.ElementAt(0),
                                Val1 = doc.ImportantDates[1],
                                ValDef = doc.ImportantDates.ElementAtOrDefault(5)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.ImportantDates.Except(new[] { new DateTime(2020, 1, 1) }).Contains(new DateTime(2020, 2, 2))
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstVal = doc.ImportantDates.First(x => x.Year > 2000),
                                LastVal = doc.ImportantDates.Last(x => x.Year < 2005),
                                SingleVal = doc.ImportantDates.Single(x => x.Year == 2010),
                                SingleOrDefaultVal = doc.ImportantDates.SingleOrDefault(x => x.Year == 1999)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                GroupCount = doc.ImportantDates.GroupBy(x => x.Year).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasCommon = doc.ImportantDates.Intersect(new[] { new DateTime(2023, 1, 1), new DateTime(2023, 12, 31) }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { new DateTime(2000, 1, 1), new DateTime(2010, 1, 1) }
                            select new
                            {
                                doc.Id,
                                JoinCount = doc.ImportantDates.Join(other, outer => outer, inner => inner, (o, i) => o).Count(),
                                GroupJoinCount = doc.ImportantDates.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/min-max-average-sum-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinYear = doc.ImportantDates.Min(x => x.Year),
                                MaxYear = doc.ImportantDates.Max(x => x.Year),
                                AvgYear = doc.ImportantDates.Average(x => x.Year),
                                SumYear = doc.ImportantDates.Sum(x => x.Year)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.ImportantDates.OfType<DateTime>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Oldest = doc.ImportantDates.OrderBy(x => x).First(),
                                Newest = doc.ImportantDates.OrderByDescending(x => x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.ImportantDates.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.ImportantDates.SequenceEqual(new[] { new DateTime(2020, 1, 1), new DateTime(2021, 1, 1) })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.ImportantDates.Skip(1).Take(1).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.ImportantDates.TakeWhile(x => x.Year < 2005).Count(),
                                CountSkipped = doc.ImportantDates.SkipWhile(x => x.Year < 2005).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DictCount = doc.ImportantDates.ToDictionary(k => k.Year, v => v.Month).Count(),
                                LookupCount = doc.ImportantDates.ToLookup(k => k.Year).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.ImportantDates.Union(new[] { new DateTime(2099, 12, 31) }).Contains(new DateTime(2099, 12, 31))
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Filter years > 2000, select Year, then sum (2001 + 2002 = 4003)
                                YearsSum = doc.ImportantDates
                                    .Where(x => x.Year > 2000)
                                    .Select(x => x.Year)
                                    .Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/date-time/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ZipEq = doc.ImportantDates.Zip(doc.ImportantDates, (a, b) => a == b).All(x => x)
                            }
                }))
        ];
    }
}
