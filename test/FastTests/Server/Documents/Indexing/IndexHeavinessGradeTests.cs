using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class IndexHeavinessGradeTests : NoDisposalNeeded
    {
        public IndexHeavinessGradeTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void SimpleMapIndex_HasLowStaticScore()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/ByCompany",
                Maps = { "from order in docs.Orders select new { order.Company }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Company", new IndexFieldOptions { Indexing = FieldIndexing.Default } }
                }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            Assert.True(grade.StaticScore <= 10, $"Expected simple index to have static score <= 10 but got {grade.StaticScore}");
            Assert.Equal(IndexStaticGrade.Simple, grade.StaticGradeLabel);
            Assert.NotNull(grade.StaticPenalties);
            Assert.True(grade.StaticPenalties.Count > 0);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MapReduceIndex_HasHigherStaticScore()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/ByCompany/Count",
                Maps = { "from order in docs.Orders select new { order.Company, Count = 1 }" },
                Reduce = "from result in results group result by result.Company into g select new { Company = g.Key, Count = g.Sum(x => x.Count) }",
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Company", new IndexFieldOptions { Indexing = FieldIndexing.Default } },
                    { "Count", new IndexFieldOptions { Indexing = FieldIndexing.Default } }
                }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // MapReduce penalty = 10 + 2 fields = 12
            Assert.True(grade.StaticScore >= 11, $"Expected map-reduce index to have static score >= 11 but got {grade.StaticScore}");
            Assert.True(grade.StaticScore <= 25, $"Expected map-reduce index to have static score <= 25 but got {grade.StaticScore}");
            Assert.Equal(IndexStaticGrade.Moderate, grade.StaticGradeLabel);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MultiMapIndex_HasExtraPenaltyPerMap()
        {
            IndexDefinition singleMapDef = new IndexDefinition
            {
                Name = "SingleMap",
                Maps = { "from o in docs.Orders select new { o.Company }" }
            };

            IndexDefinition multiMapDef = new IndexDefinition
            {
                Name = "MultiMap",
                Maps = {
                    "from o in docs.Orders select new { o.Company }",
                    "from p in docs.Products select new { Company = p.Supplier }"
                }
            };

            IndexHeavinessGrade singleGrade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(singleMapDef);
            IndexHeavinessGrade multiGrade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(multiMapDef);

            Assert.True(multiGrade.StaticScore > singleGrade.StaticScore,
                $"Multi-map should score higher than single-map. Single={singleGrade.StaticScore}, Multi={multiGrade.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void FullTextSearchField_AddsSearchPenalty()
        {
            IndexDefinition withoutSearch = new IndexDefinition
            {
                Name = "Without/Search",
                Maps = { "from doc in docs.Products select new { doc.Name }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Default } }
                }
            };

            IndexDefinition withSearch = new IndexDefinition
            {
                Name = "With/Search",
                Maps = { "from doc in docs.Products select new { doc.Name }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                }
            };

            IndexHeavinessGrade gradeWithout = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(withoutSearch);
            IndexHeavinessGrade gradeWith = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(withSearch);

            Assert.True(gradeWith.StaticScore > gradeWithout.StaticScore,
                $"Search field should increase score. Without={gradeWithout.StaticScore}, With={gradeWith.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void SpatialField_AddsHighPenalty()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Locations/Spatial",
                Maps = { "from loc in docs.Locations select new { loc.Coordinates }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Coordinates", new IndexFieldOptions { Spatial = new SpatialOptions() } }
                }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // Spatial adds 4 points. Total field baseline (1) + spatial (4) = 5
            Assert.True(grade.StaticScore >= 5, $"Spatial field should add >= 5 points, got {grade.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void VectorField_AddsHighestPenalty()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Products/Vector",
                Maps = { "from p in docs.Products select new { p.Description }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Description", new IndexFieldOptions
                        {
                            Vector = new VectorOptions { Dimensions = 768 }
                        }
                    }
                }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // Vector (8) + Vector >512 dims (4) + field baseline (1) = 13
            Assert.True(grade.StaticScore >= 13, $"High-dim vector field should add >= 13 points, got {grade.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void LoadDocument_AddsHighPenalty()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/WithCompanyName",
                Maps =
                {
                    "from order in docs.Orders let company = LoadDocument(order.Company, \"Companies\") select new { order.Id, CompanyName = company.Name }"
                }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // LoadDocument penalty = 15
            Assert.True(grade.StaticScore >= 15, $"LoadDocument should add at least 15 points, got {grade.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void OutputReduceToCollection_AddsStructuralPenalty()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/DailyCount",
                Maps = { "from order in docs.Orders select new { order.OrderedAt, Count = 1 }" },
                Reduce = "from result in results group result by result.OrderedAt into g select new { OrderedAt = g.Key, Count = g.Sum(x => x.Count) }",
                OutputReduceToCollection = "DailyOrderCounts"
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // MapReduce (10) + OutputReduceToCollection (10) + fields...
            Assert.True(grade.StaticScore >= 20, $"OutputReduceToCollection should increase score significantly, got {grade.StaticScore}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void DataScaleMultiplier_ScalesWithCollectionSize()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Products/ByName",
                Maps = { "from p in docs.Products select new { p.Name }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    { "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search } }
                }
            };

            IndexHeavinessGrade gradeSmall = IndexDefinitionHeavinessAnalyzer.ComputeFullGrade(
                definition,
                new[] { "Products" },
                stats: null,
                collectionDataProvider: _ => (500, 500 * 2048));  // 500 docs, 2KB each

            IndexHeavinessGrade gradeLarge = IndexDefinitionHeavinessAnalyzer.ComputeFullGrade(
                definition,
                new[] { "Products" },
                stats: null,
                collectionDataProvider: _ => (500_000, 500_000L * 5120));  // 500K docs, 5KB each

            Assert.True(gradeLarge.FullScore > gradeSmall.FullScore,
                $"Large collection should produce higher full score. Small={gradeSmall.FullScore}, Large={gradeLarge.FullScore}");
            Assert.True(gradeLarge.DataScaleMultiplier > gradeSmall.DataScaleMultiplier,
                $"Large collection should have higher data scale multiplier. Small={gradeSmall.DataScaleMultiplier}, Large={gradeLarge.DataScaleMultiplier}");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void HighFieldCount_AddsExtraPenaltyBeyondThreshold()
        {
            // Create an index with 12 fields (above the threshold of 10)
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Everything/Index",
                Maps = { "from doc in docs.Orders select new { doc.Company, doc.Employee, doc.ShipTo, doc.OrderedAt, doc.RequireAt, doc.ShippedAt, doc.ShipVia, doc.Freight, doc.ShipName, doc.ShipCity, doc.ShipRegion, doc.ShipCountry }" },
                Fields = new Dictionary<string, IndexFieldOptions>()
            };

            foreach (string field in new[] { "Company", "Employee", "ShipTo", "OrderedAt", "RequireAt", "ShippedAt", "ShipVia", "Freight", "ShipName", "ShipCity", "ShipRegion", "ShipCountry" })
                definition.Fields[field] = new IndexFieldOptions { Indexing = FieldIndexing.Default };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition);

            // 12 fields: 12 base + 2 extra beyond threshold of 10 = 14 from fields alone
            bool hasHighFieldCountPenalty = grade.StaticPenalties.Exists(p => p.Reason.Contains("High field count"));
            Assert.True(hasHighFieldCountPenalty, "Should have a high field count penalty for indexes with >10 fields");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ScoreLabels_AreCorrect()
        {
            void AssertLabel(IndexDefinition definition, IEnumerable<string> collections, IndexStaticGrade expectedStaticLabel)
            {
                IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition, collections);
                Assert.Equal(expectedStaticLabel, grade.StaticGradeLabel);
            }

            // Simple: 0–10
            AssertLabel(new IndexDefinition
            {
                Name = "Simple",
                Maps = { "from doc in docs.Items select new { doc.Name }" },
                Fields = new Dictionary<string, IndexFieldOptions> { { "Name", new IndexFieldOptions() } }
            }, new[] { "Items" }, IndexStaticGrade.Simple);

            // Complex via LoadDocument (15 points — Moderate range)
            AssertLabel(new IndexDefinition
            {
                Name = "Complex",
                Maps = { "from order in docs.Orders let company = LoadDocument(order.Company, \"Companies\") select new { order.Id, CompanyName = company.Name }" }
            }, new[] { "Orders" }, IndexStaticGrade.Moderate);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void StaticGrade_WithNullCollections_DoesNotThrow()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Test/Index",
                Maps = { "from doc in docs.Orders select new { doc.Company }" }
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(definition, collections: null);
            Assert.NotNull(grade);
            Assert.True(grade.StaticScore >= 0);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void RuntimePenalties_AddedForHighOutputCount()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Test/Fanout",
                Maps = { "from doc in docs.Orders select new { doc.Company }" }
            };

            IndexStats stats = new IndexStats
            {
                MaxNumberOfOutputsPerDocument = 200
            };

            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeFullGrade(definition, new[] { "Orders" }, stats, collectionDataProvider: null);

            bool hasPenalty = grade.RuntimePenalties.Exists(p => p.Reason.Contains("MaxNumberOfOutputsPerDocument"));
            Assert.True(hasPenalty, "Should have runtime penalty for high MaxNumberOfOutputsPerDocument");
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ExtractCollectionsFromMaps_QuerySyntax()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/ByCompany",
                Maps = { "from order in docs.Orders select new { order.Company }" }
            };

            System.Collections.Generic.HashSet<string> collections = IndexDefinitionHeavinessAnalyzer.ExtractCollectionsFromMaps(definition);

            Assert.NotNull(collections);
            Assert.Contains("Orders", collections);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ExtractCollectionsFromMaps_MethodSyntax()
        {
            IndexDefinition definition = new IndexDefinition
            {
                Name = "Orders/ByCompany",
                Maps = { "docs.Orders.Select(order => new { order.Company })" }
            };

            System.Collections.Generic.HashSet<string> collections = IndexDefinitionHeavinessAnalyzer.ExtractCollectionsFromMaps(definition);

            Assert.NotNull(collections);
            Assert.Contains("Orders", collections);
        }
    }
}
