using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateFloatArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/float/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SumAgg = doc.Values.Aggregate(0.0f, (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/aggregate-min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
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
                "legacy-matrix/float/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllPositive = doc.Values.All(x => x > 0f),
                                AnyGt5 = doc.Values.Any(x => x > 5.0f),
                                CountGt1 = doc.Values.Count(x => x > 1.0f)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Values.Concat(new[] { 0.5f }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.Values.Contains(1.5f)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.DefaultIfEmpty(9.9f).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt0 = doc.Values.ElementAt(0),
                                ValAtIndex1 = doc.Values[1],
                                ValAtDef = doc.Values.ElementAtOrDefault(5)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Values
                                    .Except(new[] { 1.5f })
                                    .Contains(2.5f)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstGt = doc.Values.First(x => x > 2.0f),
                                LastGt = doc.Values.Last(x => x > 2.0f),
                                SingleVal = doc.Values.Single(x => x > 4.0f), // Only 5.0 matches
                                SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x > 10.0f) // Should be null/default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by whole number part.
                                // [1.1, 1.9, 2.5, 2.9] -> Group 1 (2 items), Group 2 (2 items).
                                GroupsCount = doc.Values.GroupBy(x => (int)x).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.Values.Intersect(new[] { 1.5f, 9.9f }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 2.0f, 5.0f }
                            select new
                            {
                                doc.Id,
                                // Join on exact float equality (safe here as we use exact values).
                                // [1.0, 2.0, 3.0] join [2.0, 5.0]. Match on 2.0.
                                JoinRes = doc.Values.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),

                                // GroupJoin.
                                // 1.0 -> [], 2.0 -> [2.0], 3.0 -> [].
                                // Sum of counts = 1.
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Values.OfType<float>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
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
                "legacy-matrix/float/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { 1.5f, 2.5f })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(x => x * 2),
                                MaxVal = doc.Values.Max(x => x - 0.5f),
                                AvgVal = doc.Values.Average(x => x + 10),
                                SumVal = doc.Values.Sum(x => x * 3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
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
                "legacy-matrix/float/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.Values.TakeWhile(x => x < 5.0f).Count(),
                                CountSkipped = doc.Values.SkipWhile(x => x < 5.0f).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Key x, Value x+1
                                DictCount = doc.Values.ToDictionary(k => k, v => v + 1.0f).Count(),
                                // Lookup by integer part
                                LookupCount = doc.Values.ToLookup(k => (int)k).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Values
                                    .Union(new[] { 3.5f })
                                    .Contains(3.5f)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // > 2.0 -> [2.5, 3.5] -> * 2 -> [5.0, 7.0] -> Sum = 12.0
                                Result = doc.Values.Where(x => x > 2.0f).Select(x => x * 2).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/float/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithFloats, object>
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
