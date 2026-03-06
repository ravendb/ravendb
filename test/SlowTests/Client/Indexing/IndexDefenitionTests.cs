using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
// ReSharper disable InvokeAsExtensionMethod
// ReSharper disable CSharp14OverloadResolutionWithSpanBreakingChange

namespace SlowTests.Client.Indexing
{
    public class IndexDefinitionTests : RavenTestBase
    {
        public IndexDefinitionTests(ITestOutputHelper output) : base(output)
        {
        }

        #region Map Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = ["csharp", "ravendb"] },
                new DocWithArray { Tags = ["dotnet"] },
                new DocWithArray { Tags = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasTag = true")
                            .ToList();

                        Assert.Equal(1, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasNumber = MemoryExtensions.Contains(doc.Numbers, 42)
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Numbers = [1, 2, 42] },
                new DocWithArray { Numbers = [1, 2, 3] },
                new DocWithArray { Numbers = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasNumber = true")
                            .ToList();

                        Assert.Equal(1, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_MultipleFields_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                                  HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = ["csharp"], Categories = ["backend"] },   // both true
                new DocWithArray { Tags = ["csharp"], Categories = ["frontend"] },  // tag only
                new DocWithArray { Tags = ["java"], Categories = ["backend"] }      // category only
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Contains), map);
                    Assert.DoesNotContain(MemoryExtensionsMethodName, map);
                    Assert.DoesNotContain(ReadOnlySpanMethodName, map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Only the first doc should satisfy both conditions
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasTag = true and HasCategory = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_WithNegation_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = ["deprecated"] },
                new DocWithArray { Tags = ["active"] },
                new DocWithArray { Tags = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DoesNotHaveTag = true")
                            .ToList();

                        // two docs don't have "deprecated"
                        Assert.Equal(2, results.Count);
                    }
                });
        }

        #endregion

        #region Map Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_IntArrays_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasAnyNumber = MemoryExtensions.ContainsAny<int>(doc.Numbers, new[] { 42, 100 })
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Numbers = new[] { 1, 2, 3 } },
                new DocWithArray { Numbers = new[] { 42, 7 } },
                new DocWithArray { Numbers = new[] { 100 } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny)); // for server-side compilation
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    WaitForUserToContinueTheTest(store);
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAnyNumber = true")
                            .ToList();

                        Assert.Equal(2, results.Count); // docs with 42 or 100
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_StringArrays_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "csharp", "dotnet", "ravendb" })
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "java" } },
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "dotnet", "other" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny)); // for server-side compilation
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAnyTag = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_WithNegation_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  DoesNotHaveAnyDeprecatedTag =
                                      MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "deprecated", "obsolete" }) == false
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "deprecated" } },
                new DocWithArray { Tags = new[] { "obsolete", "other" } },
                new DocWithArray { Tags = new[] { "active" } },
                new DocWithArray { Tags = Array.Empty<string>() }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DoesNotHaveAnyDeprecatedTag = true")
                            .ToList();

                        // 2 docs that do not contain deprecated/obsolete
                        Assert.Equal(2, results.Count);
                    }
                });
        }

        #endregion

        #region MapReduce Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InMap_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "csharp", "ravendb" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalReduceAsserts: reduce =>
                    Assert.Contains(nameof(Enumerable.Sum), reduce),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.Single(results);
                        Assert.Equal("csharp", results[0].Tag);
                        Assert.Equal(2, results[0].Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InReduce_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "dotnet" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalReduceAsserts: reduce =>
                    Assert.Contains(nameof(Enumerable.Contains), reduce),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .OrderBy(x => x.Tag)
                            .ToList();

                        Assert.Equal(2, results.Count);
                        Assert.Equal("csharp", results[0].Tag);
                        Assert.Equal("dotnet", results[1].Tag);
                    }
                });
        }

        #endregion

        #region MapReduce Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InMap_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "dotnet", "other" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.Single(results);
                        Assert.Equal("important", results[0].Tag);
                        Assert.Equal(2, results[0].Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InReduce_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, CategoryCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Categories = new[] { "backend" } },
                new DocWithArray { Categories = new[] { "system" } },
                new DocWithArray { Categories = new[] { "frontend" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalReduceAsserts: reduce =>
                {
                    var hasIntersectAny = reduce.Contains(nameof(Enumerable.Intersect)) && reduce.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = reduce.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Reduce should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Reduce: '{reduce}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<CategoryCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .OrderBy(x => x.Category)
                            .ToList();

                        Assert.Equal(2, results.Count);
                        Assert.Equal("backend", results[0].Category);
                        Assert.Equal("system", results[1].Category);
                    }
                });
        }

        #endregion

        #region Mixed / Complex Usage

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                                  HasAnyImportantTag =
                                      MemoryExtensions.ContainsAny<string>(doc.Tags, new[] { "csharp", "dotnet" })
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },            // both true
                new DocWithArray { Tags = new[] { "dotnet" } },            // HasAnyImportantTag only
                new DocWithArray { Tags = new[] { "java" } }               // none
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Contains), map);
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Only first doc satisfies both flags
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasTag = true and HasAnyImportantTag = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "dotnet" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Contains), map);
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny,
                        $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.Single(results);
                        Assert.Equal("important", results[0].Tag);
                        Assert.Equal(2, results[0].Count); // two matching docs
                    }
                });
        }

        #endregion

        #region Type Coverage Tests - DateTime

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasDate = MemoryExtensions.Contains(doc.ImportantDates, new DateTime(2024, 1, 1))
                              }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = new[] { new DateTime(2024, 1, 1) } },
                new DocWithDates { ImportantDates = new[] { new DateTime(2023, 12, 31) } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasDate = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        #endregion

        #region Type Coverage Tests - Double

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasValue = MemoryExtensions.Contains(doc.Values, 3.14)
                              }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = new[] { 3.14, 2.71 } },
                new DocWithDoubles { Values = new[] { 1.0 } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasValue = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        #endregion

        #region Type Coverage Tests - Long

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasValue = MemoryExtensions.Contains(doc.Values, 42L)
                              }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = new[] { 42L, 7L } },
                new DocWithLongs { Values = new[] { 1L } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasValue = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        #endregion

        #region Edge Cases / Operators / Let

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_EmptyArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = Array.Empty<string>() }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasTag = true")
                            .ToList();

                        Assert.Empty(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithOrOperator_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  IsImportant =
                                      MemoryExtensions.Contains(doc.Tags, "csharp") ||
                                      MemoryExtensions.Contains(doc.Tags, "ravendb")
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "ravendb" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where IsImportant = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithAndOperator_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  IsImportant =
                                      MemoryExtensions.Contains(doc.Tags, "csharp") &&
                                      MemoryExtensions.Contains(doc.Tags, "ravendb")
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp", "ravendb" } },
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "ravendb" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where IsImportant = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_WithLetClause_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, object>
            {
                Map = docs => from doc in docs
                              let isImportant = MemoryExtensions.Contains(doc.Tags, "csharp")
                              select new
                              {
                                  doc.Id,
                                  IsImportant = isImportant
                              }
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where IsImportant = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_LetClauseInMap_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.Single(results);
                        Assert.Equal("important", results[0].Tag);
                        Assert.Equal(1, results[0].Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_LetClauseInReduce_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
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
            };

            var docs = new object[]
            {
                new DocWithArray { Tags = new[] { "csharp" } },
                new DocWithArray { Tags = new[] { "dotnet" } },
                new DocWithArray { Tags = new[] { "java" } }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalReduceAsserts: reduce =>
                    Assert.Contains(nameof(Enumerable.Contains), reduce),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<TagCount>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .OrderBy(x => x.Tag)
                            .ToList();

                        Assert.Equal(2, results.Count);
                        Assert.Equal("csharp", results[0].Tag);
                        Assert.Equal("dotnet", results[1].Tag);
                    }
                });
        }

        #endregion

        #region Nested Collections

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_NestedCollections_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithNestedArray, object>
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
            };

            var docs = new object[]
            {
                new DocWithNestedArray
                {
                    Items = new[]
                    {
                        new ItemWithTags { Id = "items/1-A", Tags = new[] { "csharp" } },
                        new ItemWithTags { Id = "items/2-A", Tags = new[] { "java" } }
                    }
                },
                new DocWithNestedArray
                {
                    Items = new[]
                    {
                        new ItemWithTags { Id = "items/3-A", Tags = new[] { "java" } }
                    }
                }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where IsImportant = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        #endregion

        #region Document Types

        private class DocWithArray
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
            public string[] Categories { get; set; }
            public int[] Numbers { get; set; }
        }

        private class TagCount
        {
            public string Tag { get; set; }
            public int Count { get; set; }
        }

        private class CategoryCount
        {
            public string Category { get; set; }
            public int Count { get; set; }
        }

        private class DocWithDates
        {
            public string Id { get; set; }
            public DateTime[] ImportantDates { get; set; }
        }

        private class DocWithDoubles
        {
            public string Id { get; set; }
            public double[] Values { get; set; }
        }

        private class DocWithLongs
        {
            public string Id { get; set; }
            public long[] Values { get; set; }
            public int[] IntValues { get; set; }
            public ulong[] ULongValues { get; set; }
        }

        private class ItemWithTags
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
        }

        private class DocWithNestedArray
        {
            public string Id { get; set; }
            public ItemWithTags[] Items { get; set; }
        }

        #endregion

        #region Test Helpers

        private const string MemoryExtensionsMethodName = "MemoryExtensions";
        private const string ReadOnlySpanMethodName = "ReadOnlySpan";

        private void AssertIndexBuilderRewritesAndRunsCorrectly<TDoc, TReduce>(
            Options options,
            IndexDefinitionBuilder<TDoc, TReduce> indexBuilder,
            object[] docs,
            [CallerMemberName] string indexName = null,
            Action<string> additionalMapAsserts = null,
            Action<string> additionalReduceAsserts = null,
            Action<IDocumentStore, string> additionalRunAsserts = null)
        {
            string map = null;
            string reduce = null;

            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = indexName;

                if (indexDefinition.Maps.Count != 0)
                {
                    map = indexDefinition.Maps.First();

                    Assert.False(map.Contains(MemoryExtensionsMethodName), $"Map should not contain '{MemoryExtensionsMethodName}', but it is mapped to {map}");
                    Assert.False(map.Contains(ReadOnlySpanMethodName), $"Map should not contain '{ReadOnlySpanMethodName}', but it is mapped to {map}");

                    additionalMapAsserts?.Invoke(map);
                }

                if (string.IsNullOrEmpty(indexDefinition.Reduce) == false)
                {
                    reduce = indexDefinition.Reduce;

                    Assert.DoesNotContain(MemoryExtensionsMethodName, reduce);
                    Assert.DoesNotContain(ReadOnlySpanMethodName, reduce);

                    additionalReduceAsserts?.Invoke(reduce);
                }
            }

            AssertStringBasedIndexCompilesAndRuns(
                options,
                map,
                docs,
                reduce,
                indexName,
                additionalRunAsserts);
        }

        private void AssertStringBasedIndexCompilesAndRuns(
            Options options,
            string map,
            object[] docs = null,
            string reduce = null,
            [CallerMemberName] string indexName = null,
            Action<IDocumentStore, string> additionalAsserts = null)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = indexName,
                    Maps = { map },
                    Reduce = reduce
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                if (docs != null && docs.Length > 0)
                {
                    using (var session = store.OpenSession())
                    {
                        foreach (var doc in docs)
                        {
                            session.Store(doc);
                        }

                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);
                }

                WaitForUserToContinueTheTest(store);

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexName));
                Assert.Equal(0, indexStats.ErrorsCount);

                additionalAsserts?.Invoke(store, indexName);
            }
        }

        #endregion
    }
}
