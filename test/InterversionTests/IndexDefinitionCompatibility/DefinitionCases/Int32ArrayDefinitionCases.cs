using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateInt32ArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/int/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // 0 + 1 + 2 + 3 = 6
                                SumAgg = doc.IntValues.Aggregate(0, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.IntValues.Min(),
                                MaxVal = doc.IntValues.Max(),
                                AvgVal = doc.IntValues.Average()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllPositive = doc.IntValues.All(x => x > 0),
                                AnyGt100 = doc.IntValues.Any(x => x > 100),
                                CountEven = doc.IntValues.Count(x => x % 2 == 0)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllValues = doc.IntValues.Concat(new[] { 999 }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.IntValues.Contains(42)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.IntValues.DefaultIfEmpty(99).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.IntValues.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt0 = doc.IntValues.ElementAt(0),
                                ValAtIndex1 = doc.IntValues[1],
                                ValAtDef = doc.IntValues.ElementAtOrDefault(5) // Should be default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.IntValues
                                    .Except(new[] { 1, 2 })
                                    .Contains(3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstGt = doc.IntValues.First(x => x > 10),
                                LastGt = doc.IntValues.Last(x => x > 10),
                                SingleVal = doc.IntValues.Single(x => x == 20),
                                SingleOrDefaultVal = doc.IntValues.SingleOrDefault(x => x == 999) // Should be default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by Odd/Even (x % 2).
                                // [1, 2, 3, 4] -> Key 1 has [1,3] (count 2), Key 0 has [2,4] (count 2)
                                GroupsCount = doc.IntValues.GroupBy(x => x % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.IntValues.Intersect(new[] { 42, 100 }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 1, 3, 5 }
                            select new
                            {
                                doc.Id,
                                // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                                JoinSum = doc.IntValues.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                                // GroupJoin. [1, 2, 3] into [1, 3, 5].
                                // 1 matches [1], 2 matches [], 3 matches [3].
                                // Count of matching groups that are not empty = 2.
                                GroupJoinCount = doc.IntValues.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.IntValues.OfType<int>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Smallest = doc.IntValues.OrderBy(x => x).First(),
                                Largest = doc.IntValues.OrderByDescending(x => x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.IntValues.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.IntValues.SequenceEqual(new[] { 1, 2, 3 })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.IntValues.Min(x => x * 2),
                                MaxVal = doc.IntValues.Max(x => x - 5),
                                AvgVal = doc.IntValues.Average(x => x + 10),
                                SumVal = doc.IntValues.Sum(x => x * 3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Skip 1, Take 1. [10, 20, 30] -> [20] -> Sum = 20
                                Value = doc.IntValues.Skip(1).Take(1).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.IntValues.TakeWhile(x => x < 10).Count(),
                                CountSkipped = doc.IntValues.SkipWhile(x => x < 10).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Key is value, Value is value*10.
                                DictCount = doc.IntValues.ToDictionary(k => k, v => v * 10).Count(),
                                // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                                LookupCount = doc.IntValues.ToLookup(k => k % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.IntValues
                                    .Union(new[] { 3 })
                                    .Contains(3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Filter > 10, then multiply by 2, then sum
                                Result = doc.IntValues.Where(x => x > 10).Select(x => x * 2).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/int/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                                ZipFirst = doc.IntValues.Zip(doc.IntValues, (a, b) => a + b).First()
                            }
                }))
        ];
    }
}
