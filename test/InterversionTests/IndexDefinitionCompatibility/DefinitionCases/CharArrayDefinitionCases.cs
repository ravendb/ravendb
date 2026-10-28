using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateCharArrayCases()
    {
        return
        [
            new DefinitionCase(
                "legacy-matrix/char/aggregate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Concat chars to string
                                Str = doc.Values.Aggregate("", (acc, c) => acc + c)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/any-all-count-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                AllLetters = doc.Values.All(c => char.IsLetter(c)),
                                AnyDigit = doc.Values.Any(c => char.IsDigit(c)),
                                CountVowels = doc.Values.Count(c => c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/average-with-selector",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Avg = doc.Values.Average(c => c)
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/concat",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                TotalLen = doc.Values.Concat(new[] { '!' }).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/contains",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasVal = doc.Values.Contains('!')
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/default-if-empty",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstVal = doc.Values.DefaultIfEmpty('!').First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/distinct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DistinctCount = doc.Values.Distinct().Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/element-at-indexer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                ValAtIndex = doc.Values.ElementAt(1),
                                ValAtIdx = doc.Values[1],
                                ValDef = doc.Values.ElementAtOrDefault(10) // should be default char \0
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/except",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasRemaining = doc.Values
                                    .Except(new[] { 'a' })
                                    .Contains('b')
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/first-last-single-with-predicate",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstMatch = doc.Values.First(c => c > 'a'),
                                LastMatch = doc.Values.Last(c => c < 'z'),
                                SingleMatch = doc.Values.Single(c => c == 'b'),
                                SingleDefMatch = doc.Values.SingleOrDefault(c => c == '!') // null/default
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/group-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Groups = doc.Values.GroupBy(c => char.IsDigit(c)).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/intersect",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasCommon = doc.Values.Intersect(new[] { 'a', 'z' }).Any()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/join-group-join",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            let other = new[] { 'a', 'c' }
                            select new
                            {
                                doc.Id,
                                JoinCount = doc.Values.Join(other, o => o, i => i, (o, i) => o).Count(),
                                GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/min-max",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                MinVal = doc.Values.Min(),
                                MaxVal = doc.Values.Max()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/order-by",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                FirstSorted = doc.Values.OrderBy(c => c).First(),
                                LastSorted = doc.Values.OrderByDescending(c => c).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/reverse",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                LastAfterReverse = doc.Values.Reverse().Last()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/sequence-equal",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                IsExact = doc.Values.SequenceEqual(new[] { 'x', 'y' })
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/take-skip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Val = doc.Values.Skip(1).Take(1).First()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/take-while-skip-while",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                Taken = doc.Values.TakeWhile(c => c < 'c').Count(),
                                Skipped = doc.Values.SkipWhile(c => c < 'c').Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/to-dictionary-to-lookup",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                DictSize = doc.Values.ToDictionary(c => c.ToString(), c => (int)c).Count(),
                                LookupSize = doc.Values.ToLookup(c => char.IsDigit(c)).Count()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/union",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                HasUnionValue = doc.Values
                                    .Union(new[] { 'z' })
                                    .Contains('z')
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/where-select",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Digits only, then cast to int, then sum. '1'=49, '2'=50. Sum=99.
                                SumDigits = doc.Values.Where(c => char.IsDigit(c)).Select(c => (int)c).Sum()
                            }
                })),
            new DefinitionCase(
                "legacy-matrix/char/zip",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithChars, object>
                    {
                        Map = docs => from doc in docs
                            select new
                            {
                                doc.Id,
                                // Zip 'a','b' with 'a','b' => "aa", "bb". First is "aa"
                                Zipped = doc.Values.Zip(doc.Values, (a, b) => "" + a + b).First()
                            }
                }))
        ];
    }
}
