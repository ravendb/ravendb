using System;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateDoubleArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/double/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SumAgg = doc.Values.Aggregate(0.0, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(),
                                MaxVal = doc.Values.Max(),
                                AvgVal = doc.Values.Average()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllPositive = doc.Values.All(x => x > 0),
                                AnyGt5 = doc.Values.Any(x => x > 5.0),
                                CountGt2 = doc.Values.Count(x => x > 2.0)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Values.Concat(new[] { 0.1 }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.Values.Contains(1.1)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.DefaultIfEmpty(9.9).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt0 = doc.Values.ElementAt(0),
                                ValAtIndex1 = doc.Values[1],
                                ValAtDef = doc.Values.ElementAtOrDefault(5) // Should be default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Values
                                    .Except(new[] { 1.1 })
                                    .Contains(2.2)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstGt = doc.Values.First(x => x > 2.0),
                                LastGt = doc.Values.Last(x => x > 2.0),
                                // ReSharper disable once CompareOfFloatsByEqualityOperator
                                SingleVal = doc.Values.Single(x => x == 3.3),
                                // ReSharper disable once CompareOfFloatsByEqualityOperator
                                SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 99.9) // Should be default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by Math.Floor. 1.1 and 1.9 -> 1 (count 2). 2.2 -> 2 (count 1).
                                GroupsCount = doc.Values.GroupBy(x => Math.Floor(x)).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.Values.Intersect(new[] { 1.1, 9.9 }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 1.1, 3.3, 5.5 }
                            select new
                            {
                                doc.Id,
                                // Join on equality.
                                JoinSum = doc.Values.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                                // GroupJoin.
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Values.OfType<double>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Smallest = doc.Values.OrderBy(x => x).First(),
                                Largest = doc.Values.OrderByDescending(x => x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { 1.1, 2.2 })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(x => x * 2),
                                MaxVal = doc.Values.Max(x => x - 0.5),
                                AvgVal = doc.Values.Average(x => x + 10),
                                SumVal = doc.Values.Sum(x => x * 2)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Skip 1, Take 1. [1.1, 2.2, 3.3] -> [2.2]
                                Val = doc.Values.Skip(1).Take(1).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.Values.TakeWhile(x => x < 3.0).Count(),
                                CountSkipped = doc.Values.SkipWhile(x => x < 3.0).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count(),
                                // Lookup by Math.Floor. [1.1, 1.9, 2.2] -> Keys: 1 (vals 1.1,1.9), 2 (val 2.2)
                                LookupCount = doc.Values.ToLookup(k => Math.Floor(k)).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Values
                                    .Union(new[] { 3.3 })
                                    .Contains(3.3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Filter > 2.0, multiply by 2, sum. [1.1, 3.3, 4.4] -> [3.3, 4.4] -> [6.6, 8.8] -> 15.4
                                Result = doc.Values.Where(x => x > 2.0).Select(x => x * 2.0).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/double/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ZipSum = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                            }
                }))
        ];
    }
}
