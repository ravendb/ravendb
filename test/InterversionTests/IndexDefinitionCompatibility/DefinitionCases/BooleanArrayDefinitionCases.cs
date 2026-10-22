using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateBooleanArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/bool/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Logical AND all items. true & true & false = false
                                AllAnd = doc.Values.Aggregate(true, (acc, val) => acc & val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllTrue = doc.Values.All(x => x),
                                AnyFalse = doc.Values.Any(x => !x),
                                CountTrue = doc.Values.Count(x => x)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Values.Concat(new[] { true }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasFalse = doc.Values.Contains(false)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.DefaultIfEmpty(true).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAt0 = doc.Values.ElementAt(0),
                                ValAtIndex1 = doc.Values[1],
                                ValAtDef = doc.Values.ElementAtOrDefault(5) // Default for bool is false
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // [true, false] except [false] -> [true]
                                OnlyTrueRemains = doc.Values.Except(new[] { false }).Single()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstTrue = doc.Values.First(x => x == true),
                                LastFalse = doc.Values.Last(x => x == false),
                                SingleTrue = doc.Values.Single(x => x == true),
                                SingleOrDefault = doc.Values.SingleOrDefault(x => x == true) // Should succeed if only 1 true
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Group by self. [true, false, true] -> Keys: true (2), false (1)
                                GroupsCount = doc.Values.GroupBy(x => x).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasTrue = doc.Values.Intersect(new[] { true }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { true }
                            select new
                            {
                                doc.Id,
                                // Join [true, false] with [true]. Match on true.
                                JoinCount = doc.Values.Join(other, o => o, i => i, (o, i) => o).Count(),

                                // GroupJoin.
                                // true matches [true], false matches [].
                                // Count of groups with elements = 1.
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Values.OfType<bool>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstSorted = doc.Values.OrderBy(x => x).First(),
                                LastSorted = doc.Values.OrderByDescending(x => x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { true, false })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SumVal = doc.Values.Sum(x => x ? 1 : 0),
                                MinVal = doc.Values.Min(x => x ? 1 : 0),
                                MaxVal = doc.Values.Max(x => x ? 1 : 0),
                                AvgVal = doc.Values.Average(x => x ? 1.0 : 0.0)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.Skip(1).Take(1).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                CountTaken = doc.Values.TakeWhile(x => x).Count(),
                                CountSkipped = doc.Values.SkipWhile(x => x).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Keys: true, false
                                DictCount = doc.Values.ToDictionary(k => k, v => v).Count(),
                                // Lookup: true group, false group
                                LookupCount = doc.Values.ToLookup(k => k).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // [false] union [true] -> [false, true] (order depends on impl, but distinct items)
                                Count = doc.Values.Union(new[] { true }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Filter true, then invert to false
                                Result = doc.Values.Where(x => x).Select(x => !x).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/bool/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithBools, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Zip with self, XOR pairs. [true, false] zip [true, false] -> (t^t=f, f^f=f) -> All false
                                AnyTrue = doc.Values.Zip(doc.Values, (a, b) => a ^ b).Any(x => x)
                            }
                }))
        ];
    }
}
