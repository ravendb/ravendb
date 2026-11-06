using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateStringArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/string/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Concatenated = doc.Tags.Aggregate("", (acc, val) => acc + val)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllStartWithA = doc.Tags.All(x => x.StartsWith("a")),
                                AnyLen3 = doc.Tags.Any(x => x.Length == 3),
                                CountLen5 = doc.Tags.Count(x => x.Length == 5)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Tags.Concat(new[] { "c" }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasValue = doc.Tags.Contains("b")
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Tags.DefaultIfEmpty("default").First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Tags.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAtIndex = doc.Tags[1],
                                ValAt = doc.Tags.ElementAt(1),
                                ValAtDef = doc.Tags.ElementAtOrDefault(3)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Tags
                                    .Except(new[] { "a" })
                                    .Contains("b")
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstMatch = doc.Tags.First(x => x.StartsWith("b")),
                                LastMatch = doc.Tags.Last(x => x.Length == 3),
                                SingleMatch = doc.Tags.Single(x => x == "apple"),
                                SingleDef = doc.Tags.SingleOrDefault(x => x == "nonexistent")
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                GroupCount = doc.Tags.GroupBy(x => x.Length).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasAny = doc.Tags.Intersect(new[] { "a", "z" }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { "b", "c" }
                            select new
                            {
                                doc.Id,
                                JoinRes = doc.Tags.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),
                                GroupJoinCount = doc.Tags.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinTag = doc.Tags.Min(),
                                MaxTag = doc.Tags.Max()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/of-type",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // ReSharper disable once RedundantEnumerableCastCall
                                OfTypeCount = doc.Tags.OfType<string>().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                SortedLast = doc.Tags.OrderBy(x => x).Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstAfterReverse = doc.Tags.Reverse().First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Tags.SequenceEqual(new[] { "a", "b" })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/sum-average-min-max-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinLen = doc.Tags.Min(x => x.Length),
                                MaxLen = doc.Tags.Max(x => x.Length),
                                AvgLen = doc.Tags.Average(x => x.Length),
                                SumLen = doc.Tags.Sum(x => x.Length)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Tags.Skip(1).Take(1).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Taken = doc.Tags.TakeWhile(x => x.Length < 4).Count(),
                                Skipped = doc.Tags.SkipWhile(x => x.Length < 4).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Key is string, Value is Length.
                                DictCount = doc.Tags.ToDictionary(k => k, v => v.Length).Count(),
                                // Lookup by first char.
                                LookupCount = doc.Tags.ToLookup(k => k[0]).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Tags
                                    .Union(new[] { "c" })
                                    .Contains("c")
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/where",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // StartsWith 'a'
                                // ReSharper disable once ReplaceWithSingleCallToCount
                                ACount = doc.Tags.Where(x => x.StartsWith("a")).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/string/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithStrings, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ZipVal = doc.Tags.Zip(doc.Tags, (a, b) => a + b).First()
                            }
                }))
        ];
    }
}
