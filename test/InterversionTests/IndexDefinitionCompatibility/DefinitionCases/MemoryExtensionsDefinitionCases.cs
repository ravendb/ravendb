using System;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static partial class GeneratedDefinitionCases
{
    private static DefinitionCase[] CreateMemoryExtensionsCases()
    {
        return
        [
            new DefinitionCase(
                "current-merged/date-time/map-index-memory-extensions-contains-date-time-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDates, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasDate = MemoryExtensions.Contains(doc.ImportantDates, new DateTime(2024, 1, 1))
                                      }
                })),
            new DefinitionCase(
                "current-merged/double/map-index-memory-extensions-contains-double-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithDoubles, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasValue = MemoryExtensions.Contains(doc.Values, 3.14)
                                      }
                })),
            new DefinitionCase(
                "current-merged/int/map-index-memory-extensions-contains-any-int-arrays-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasAnyNumber = MemoryExtensions.ContainsAny<int>(doc.Numbers, new[] { 42, 100 })
                                      }
                })),
            new DefinitionCase(
                "current-merged/int/map-index-memory-extensions-contains-int-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasNumber = MemoryExtensions.Contains(doc.Numbers, 42)
                                      }
                })),
            new DefinitionCase(
                "current-merged/long/map-index-memory-extensions-contains-long-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithLongs, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasValue = MemoryExtensions.Contains(doc.Values, 42L)
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-contains-any-with-negation-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          DoesNotHaveAnyDeprecatedTag =
                                              MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "deprecated", "obsolete" }) == false
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-contains-empty-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-contains-multiple-fields-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                                          HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-contains-with-negation-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-with-and-operator-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          IsImportant =
                                              MemoryExtensions.Contains(doc.Tags, "csharp") &&
                                              MemoryExtensions.Contains(doc.Tags, "ravendb")
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-memory-extensions-with-or-operator-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          IsImportant =
                                              MemoryExtensions.Contains(doc.Tags, "csharp") ||
                                              MemoryExtensions.Contains(doc.Tags, "ravendb")
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-mixed-memory-extensions-calls-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                                          HasAnyImportantTag =
                                              MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "csharp", "dotnet" })
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-index-with-let-clause-memory-extensions-contains-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      let isImportant = MemoryExtensions.Contains(doc.Tags, "csharp")
                                      select new
                                      {
                                          doc.Id,
                                          IsImportant = isImportant
                                      }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-let-clause-in-map-memory-extensions-contains-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      let isImportant = MemoryExtensions.Contains(doc.Tags, "csharp")
                                      where isImportant
                                      select new TagCount
                                      {
                                          Tag = "important",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-let-clause-in-reduce-memory-extensions-contains-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      select new TagCount
                                      {
                                          Tag = doc.Tags.FirstOrDefault() ?? "none",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            let isImportant = MemoryExtensions.Contains(new[] { "csharp", "dotnet" }, g.Key)
                                            where isImportant
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-memory-extensions-contains-any-in-map-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      where MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "csharp", "dotnet", "ravendb" })
                                      select new TagCount
                                      {
                                          Tag = "important",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-memory-extensions-contains-any-in-reduce-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, CategoryCount>
                    {
                        Map = docs => from doc in docs
                                      select new CategoryCount
                                      {
                                          Category = doc.Categories.FirstOrDefault() ?? "none",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Category
                            into g
                                            where MemoryExtensions.ContainsAny<string>(new[] { "backend", "system" }, new[] { g.Key })
                                            select new CategoryCount
                                            {
                                                Category = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-memory-extensions-contains-in-map-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      where MemoryExtensions.Contains(doc.Tags, "csharp")
                                      select new TagCount
                                      {
                                          Tag = "csharp",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-memory-extensions-contains-in-reduce-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      select new TagCount
                                      {
                                          Tag = doc.Tags.FirstOrDefault(),
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            where MemoryExtensions.Contains(new[] { "include-csharp", "include-dotnet" }, "include-" + g.Key)
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/mixed/map-reduce-index-mixed-memory-extensions-calls-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, TagCount>
                    {
                        Map = docs => from doc in docs
                                      where MemoryExtensions.Contains(doc.Tags, "csharp")
                                            || MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "dotnet" })
                                      select new TagCount
                                      {
                                          Tag = "important",
                                          Count = 1
                                      },
                        Reduce = results => from result in results
                                            group result by result.Tag
                            into g
                                            select new TagCount
                                            {
                                                Tag = g.Key,
                                                Count = g.Sum(x => x.Count)
                                            }
                })),
            new DefinitionCase(
                "current-merged/string/map-index-memory-extensions-contains-any-string-arrays-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "csharp", "dotnet", "ravendb" })
                                      }
                })),
            new DefinitionCase(
                "current-merged/string/map-index-memory-extensions-contains-string-array-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithArray, object>
                    {
                        Map = docs => from doc in docs
                                      select new
                                      {
                                          doc.Id,
                                          HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                                      }
                })),
            new DefinitionCase(
                "current-merged/string/map-index-memory-extensions-nested-collections-should-work",
                conventions => Definition(conventions, new IndexDefinitionBuilder<DocWithNestedArray, object>
                    {
                        Map = docs => from doc in docs
                                      from item in doc.Items
                                      where MemoryExtensions.Contains(item.Tags, "csharp")
                                      select new
                                      {
                                          doc.Id,
                                          ItemId = item.Id,
                                          IsImportant = true
                                      }
                }))
        ];
    }
}
