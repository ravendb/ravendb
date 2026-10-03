using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateDecimalArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/decimal/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SumAgg = doc.Values.Aggregate(0.0m, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/aggregate-sum-avg-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SumVal = doc.Values.Sum(),
                                AvgVal = doc.Values.Average(),
                                MinVal = doc.Values.Min(),
                                MaxVal = doc.Values.Max()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasBig = doc.Values.Any(x => x > 5.0m),
                                AllPos = doc.Values.All(x => x > 0m),
                                CountPos = doc.Values.Count(x => x > 1.5m)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Values.Concat(new[] { 9.9m }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/contains-distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasVal = doc.Values.Contains(2.2m),
                                UniqueCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.DefaultIfEmpty(9.9m).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt1 = doc.Values.ElementAt(1),
                                ValAtIdx = doc.Values[0],
                                ValDef = doc.Values.ElementAtOrDefault(5)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Values.Except(new[] { 1.1m }).Contains(2.2m)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstVal = doc.Values.First(x => x > 2.0m),
                                LastVal = doc.Values.Last(x => x > 2.0m),
                                SingleVal = doc.Values.Single(x => x == 2.2m),
                                SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 9.9m)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by integer part. [1.1, 1.9, 2.5] -> Key 1 (2 items), Key 2 (1 item)
                                GroupsCount = doc.Values.GroupBy(x => (int)x).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.Values.Intersect(new[] { 1.1m, 9.9m }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 2.0m, 5.0m }
                            select new
                            {
                                doc.Id,
                                // Join [1, 2, 3] with [2, 5]. Match is 2.
                                JoinRes = doc.Values.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),
                                // GroupJoin.
                                // 1 matches [], 2 matches [2], 3 matches [].
                                // Count of non-empty groups = 1.
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Values.OfType<decimal>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
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
                "legacy-matrix/decimal/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { 1.1m, 2.2m })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(x => x * 2m),
                                MaxVal = doc.Values.Max(x => x - 5m),
                                AvgVal = doc.Values.Average(x => x + 10m),
                                SumVal = doc.Values.Sum(x => x * 3m)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.Skip(1).Take(1).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.Values.TakeWhile(x => x < 5.0m).Count(),
                                CountSkipped = doc.Values.SkipWhile(x => x < 5.0m).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count(),
                                // Lookup by integer part. [1.1, 1.9, 2.5] -> Keys: 1, 2
                                LookupCount = doc.Values.ToLookup(k => (int)k).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Values.Union(new[] { 3.3m }).Contains(3.3m)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountBig = doc.Values.Where(x => x > 5.0m).Select(x => x * 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/decimal/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDecimals, object>
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
