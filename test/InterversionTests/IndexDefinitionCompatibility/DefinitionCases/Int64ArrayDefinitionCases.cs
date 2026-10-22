using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateInt64ArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/long/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // 0 + 1 + 2 + 3 = 6
                                SumAgg = doc.Values.Aggregate(0L, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
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
                "legacy-matrix/long/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllPositive = doc.Values.All(x => x > 0),
                                AnyGt100 = doc.Values.Any(x => x > 100),
                                CountEven = doc.Values.Count(x => x % 2 == 0)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllValues = doc.Values.Concat(new[] { 999L }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.Values.Contains(42L)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.DefaultIfEmpty(99L).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
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
                "legacy-matrix/long/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Values
                                    .Except(new[] { 1L, 2L })
                                    .Contains(3L)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstGt = doc.Values.First(x => x > 10),
                                LastGt = doc.Values.Last(x => x > 10),
                                SingleVal = doc.Values.Single(x => x == 20),
                                SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 999) // Should be default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by Odd/Even (x % 2).
                                // [1, 2, 3, 4] -> Key 1 has [1,3] (count 2), Key 0 has [2,4] (count 2)
                                GroupsCount = doc.Values.GroupBy(x => x % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.Values.Intersect(new[] { 42L, 100L }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 1L, 3L, 5L }
                            select new
                            {
                                doc.Id,
                                // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                                JoinSum = doc.Values.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                                // GroupJoin. [1, 2, 3] into [1, 3, 5].
                                // 1 matches [1], 2 matches [], 3 matches [3].
                                // Count of matching groups that are not empty = 2.
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Values.OfType<long>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
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
                "legacy-matrix/long/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { 1L, 2L, 3L })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(x => x * 2),
                                MaxVal = doc.Values.Max(x => x - 5),
                                AvgVal = doc.Values.Average(x => x + 10),
                                SumVal = doc.Values.Sum(x => x * 3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Skip 1, Take 1. [10, 20, 30] -> [20] -> Sum = 20
                                Value = doc.Values.Skip(1).Take(1).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.Values.TakeWhile(x => x < 10L).Count(),
                                CountSkipped = doc.Values.SkipWhile(x => x < 10L).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Key is value, Value is value*10.
                                DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count(),
                                // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                                LookupCount = doc.Values.ToLookup(k => k % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Values
                                    .Union(new[] { 3L })
                                    .Contains(3L)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Filter > 10, then multiply by 2, then sum
                                Result = doc.Values.Where(x => x > 10L).Select(x => x * 2).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/long/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                                ZipFirst = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                            }
                }))
        ];
    }
}
