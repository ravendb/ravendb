using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateUInt64ArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/ulong/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // 0 + 1 + 2 + 3 = 6
                                SumAgg = doc.ULongValues.Aggregate(0UL, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.ULongValues.Min(),
                                MaxVal = doc.ULongValues.Max(),
                                // Average on ulong requires casting usually, or let's see if dynamic handles it if we cast
                                AvgVal = doc.ULongValues.Select(x => (decimal)x).Average()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllPositive = doc.ULongValues.All(x => x > 0),
                                AnyGt100 = doc.ULongValues.Any(x => x > 100),
                                CountEven = doc.ULongValues.Count(x => x % 2 == 0)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllValues = doc.ULongValues.Concat(new[] { 999UL }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.ULongValues.Contains(42UL)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.ULongValues.DefaultIfEmpty(99UL).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.ULongValues.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt0 = doc.ULongValues.ElementAt(0),
                                ValAtIndex1 = doc.ULongValues[1],
                                ValAtDef = doc.ULongValues.ElementAtOrDefault(5)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.ULongValues
                                    .Except(new[] { 1UL, 2UL })
                                    .Contains(3UL)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstGt = doc.ULongValues.First(x => x > 10),
                                LastGt = doc.ULongValues.Last(x => x > 10),
                                SingleVal = doc.ULongValues.Single(x => x == 20),
                                SingleOrDefaultVal = doc.ULongValues.SingleOrDefault(x => x == 999)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by Odd/Even (x % 2).
                                GroupsCount = doc.ULongValues.GroupBy(x => x % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.ULongValues.Intersect(new[] { 42UL, 100UL }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 1UL, 3UL, 5UL }
                            select new
                            {
                                doc.Id,
                                // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                                JoinSum = doc.ULongValues.Join(other, outer => outer, inner => inner, (o, i) => (decimal)o).Sum(),

                                // GroupJoin. [1, 2, 3] into [1, 3, 5].
                                // 1 matches [1], 2 matches [], 3 matches [3].
                                // Count of matching groups that are not empty = 2.
                                GroupJoinCount = doc.ULongValues.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.ULongValues.OfType<ulong>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Smallest = doc.ULongValues.OrderBy(x => x).First(),
                                Largest = doc.ULongValues.OrderByDescending(x => x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.ULongValues.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.ULongValues.SequenceEqual(new[] { 1UL, 2UL, 3UL })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.ULongValues.Min(x => x * 2), // 20, 40
                                MaxVal = doc.ULongValues.Max(x => x - 5), // 5, 15
                                // For Sum/Average on ulongs, it's safer to cast to decimal to avoid compilation issues or missing overloads
                                AvgVal = doc.ULongValues.Average(x => (decimal)x + 10), // 20, 30 -> 25
                                SumVal = doc.ULongValues.Sum(x => (decimal)x * 3) // 30, 60 -> 90
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Skip 1, Take 1. [10, 20, 30] -> [20] -> Cast to decimal to Sum
                                Value = doc.ULongValues.Skip(1).Take(1).Sum(x => (decimal)x)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.ULongValues.TakeWhile(x => x < 10UL).Count(),
                                CountSkipped = doc.ULongValues.SkipWhile(x => x < 10UL).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Key is value, Value is value*10.
                                DictCount = doc.ULongValues.ToDictionary(k => k, v => v * 10).Count(),
                                // Lookup by Modulo 2.
                                LookupCount = doc.ULongValues.ToLookup(k => k % 2).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.ULongValues
                                    .Union(new[] { 3UL })
                                    .Contains(3UL)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Result = doc.ULongValues.Where(x => x > 10UL).Select(x => (decimal)x * 2).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/ulong/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                                ZipFirst = doc.ULongValues.Zip(doc.ULongValues, (a, b) => a + b).First()
                            }
                }))
        ];
    }
}
