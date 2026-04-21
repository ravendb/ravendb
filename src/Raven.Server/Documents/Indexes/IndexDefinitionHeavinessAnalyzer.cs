using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Client;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes
{
    /// <summary>
    /// Computes the Index Heaviness Grade based on the index definition (static score)
    /// and optionally data scale and runtime observations (full score).
    ///
    /// Static score — based purely on the IndexDefinition, server-independent.
    /// Full score   — StaticScore × DataScaleMultiplier + RuntimePenalties.
    ///
    /// All penalty values are intentionally hardcoded constants that evolve through code changes.
    /// </summary>
    internal static class IndexDefinitionHeavinessAnalyzer
    {
        // ───────────────────────── Label thresholds ─────────────────────────
        // Static
        private const int StaticSimpleMax = 10;
        private const int StaticModerateMax = 25;
        private const int StaticComplexMax = 50;

        // Full
        private const double FullLightMax = 20;
        private const double FullModerateMax = 80;
        private const double FullHeavyMax = 200;
        private const double FullVeryHeavyMax = 500;

        // ─────────────────── Tier 1: Static penalty constants ───────────────

        // 1.1 Index type base cost
        private const int PenaltyMapReduce = 10;
        private const int PenaltyJavaScript = 5;
        private const int PenaltyExtraMap = 3;         // per extra map beyond 1
        private const int PenaltyTimeSeriesSource = 3;
        private const int PenaltyCountersSource = 2;

        // 1.2 Field penalties
        private const int PenaltyFieldBase = 1;
        private const int PenaltyFieldStorage = 1;
        private const int PenaltyFieldSearch = 2;
        private const int PenaltyFieldCustomAnalyzer = 1;
        private const int PenaltyFieldSuggestions = 3;
        private const int PenaltyFieldTermVector = 1;
        private const int PenaltyFieldTermVectorPositionsAndOffsets = 2;  // replaces +1
        private const int PenaltyFieldSpatial = 4;
        private const int PenaltyFieldSpatialQuadTree = 2;                // on top of spatial
        private const int PenaltyFieldVector = 8;
        private const int PenaltyFieldVectorHighDimensions = 4;           // on top of vector, if > 512
        private const int HighFieldCountThreshold = 10;
        private const int PenaltyHighFieldCountPerExtra = 2;

        // 1.3 Map complexity penalties
        private const int PenaltyLoadDocument = 15;
        private const int PenaltyExtraLoadDocument = 5;  // per extra beyond 1
        private const int PenaltyFanout = 8;
        private const int PenaltyNestedFanout = 10;
        private const int LetClauseThreshold = 3;
        private const int PenaltyExtraLetClause = 2;      // per extra beyond 3
        private const int PenaltyWhere = 1;
        private const int PenaltyRecurse = 10;

        private const int PenaltyTooComplexToAnalyze = 20;

        // 1.4 Structural penalties
        private const int PenaltyOutputReduceToCollection = 10;
        private const int PenaltyPatternForOutputReduceReferences = 3;
        private const int PenaltyAdditionalSourcesBase = 3;
        private const int PenaltyAdditionalSourcesPerFile = 1;
        private const int PenaltyAdditionalAssembliesBase = 5;
        private const int PenaltyAdditionalAssembliesPerAssembly = 2;
        private const int PenaltyAllDocsCollection = 15;
        private const int PenaltyArchivedDataProcessing = 2;

        // ─────────────────── Tier 2: Runtime penalty constants ──────────────
        private const int PenaltyMaxOutputsHigh = 5;       // > 100
        private const int PenaltyMaxOutputsVeryHigh = 10;  // > 1024 (on top)
        private const double ErrorRateThresholdHigh = 0.05;
        private const double ErrorRateThresholdInvalid = 0.15;
        private const int PenaltyErrorRateHigh = 5;
        private const int PenaltyErrorRateInvalid = 10;

        // ─────────────────── Collection size factors ────────────────────────
        private static double GetCollectionSizeFactor(long count)
        {
            if (count < 1_000) return 0.5;
            if (count < 10_000) return 1.0;
            if (count < 100_000) return 2.0;
            if (count < 1_000_000) return 4.0;
            if (count < 10_000_000) return 8.0;
            return 15.0;
        }

        // ─────────────────── Document size factors ──────────────────────────
        private static double GetDocumentSizeFactor(long avgDocSizeBytes)
        {
            if (avgDocSizeBytes < 1_024) return 0.8;
            if (avgDocSizeBytes < 10 * 1_024) return 1.0;
            if (avgDocSizeBytes < 100 * 1_024) return 1.5;
            if (avgDocSizeBytes < 1_024 * 1_024) return 2.5;
            return 4.0;
        }

        /// <summary>
        /// Computes the static-only heaviness grade from an index definition.
        /// No server context required. Suitable for pre-flight analysis.
        /// </summary>
        /// <param name="definition">The index definition to analyze.</param>
        /// <param name="collections">
        /// Optional collection names indexed by this index. When null, the @all_docs structural penalty
        /// will be skipped. Use <see cref="ExtractCollectionsFromMaps"/> to populate this from the definition maps.
        /// </param>
        public static IndexHeavinessGrade ComputeStaticGrade(IndexDefinition definition, IEnumerable<string> collections = null)
        {
            var staticPenalties = new List<IndexHeavinessPenalty>();
            int staticScore = ComputeStaticScore(definition, collections, staticPenalties);

            return new IndexHeavinessGrade
            {
                StaticScore = staticScore,
                FullScore = staticScore,
                StaticGradeLabel = GetStaticLabel(staticScore),
                FullGradeLabel = GetFullLabel(staticScore),
                DataScaleMultiplier = 1.0,
                StaticPenalties = staticPenalties,
                RuntimePenalties = new List<IndexHeavinessPenalty>()
            };
        }

        /// <summary>
        /// Computes the full heaviness grade including data scale modifiers and runtime observations.
        /// This convenience overload recomputes the static grade on every call (involves Roslyn parsing).
        /// For repeated calls on the same index definition, prefer caching the static grade via
        /// <see cref="ComputeStaticGrade"/> and using the <see cref="ComputeFullGrade(IndexHeavinessGrade, IEnumerable{string}, IndexStats, CollectionDataProvider)"/> overload.
        /// </summary>
        /// <param name="definition">The index definition.</param>
        /// <param name="collections">Collections indexed by this index (from Index.Collections).</param>
        /// <param name="stats">Current runtime stats for the index, used for runtime penalties.</param>
        /// <param name="collectionDataProvider">Delegate to retrieve per-collection document count and total size.</param>
        public static IndexHeavinessGrade ComputeFullGrade(
            IndexDefinition definition,
            IEnumerable<string> collections,
            IndexStats stats,
            CollectionDataProvider collectionDataProvider)
        {
            IndexHeavinessGrade staticGrade = ComputeStaticGrade(definition, collections);
            return ComputeFullGrade(staticGrade, collections, stats, collectionDataProvider);
        }

        /// <summary>
        /// Computes the full heaviness grade using a pre-computed static grade.
        /// Avoids re-parsing map expressions when the static grade is cached.
        /// </summary>
        /// <param name="staticGrade">A previously computed static grade (from <see cref="ComputeStaticGrade"/>).</param>
        /// <param name="collections">Collections indexed by this index (from Index.Collections).</param>
        /// <param name="stats">Current runtime stats for the index, used for runtime penalties.</param>
        /// <param name="collectionDataProvider">Delegate to retrieve per-collection document count and total size.</param>
        public static IndexHeavinessGrade ComputeFullGrade(
            IndexHeavinessGrade staticGrade,
            IEnumerable<string> collections,
            IndexStats stats,
            CollectionDataProvider collectionDataProvider)
        {
            int staticScore = staticGrade.StaticScore;

            double dataScaleMultiplier = 1.0;
            if (collectionDataProvider != null)
                dataScaleMultiplier = ComputeDataScaleMultiplier(collections, collectionDataProvider);

            var runtimePenalties = new List<IndexHeavinessPenalty>();
            double runtimeScore = 0;
            if (stats != null)
                runtimeScore = ComputeRuntimePenalties(stats, runtimePenalties);

            double fullScore = staticScore * dataScaleMultiplier + runtimeScore;

            return new IndexHeavinessGrade
            {
                StaticScore = staticScore,
                FullScore = Math.Round(fullScore, 2),
                StaticGradeLabel = staticGrade.StaticGradeLabel,
                FullGradeLabel = GetFullLabel(fullScore),
                DataScaleMultiplier = Math.Round(dataScaleMultiplier, 2),
                StaticPenalties = staticGrade.StaticPenalties,
                RuntimePenalties = runtimePenalties
            };
        }

        private static int ComputeStaticScore(IndexDefinition definition, IEnumerable<string> collections, List<IndexHeavinessPenalty> penalties)
        {
            int score = 0;

            // 1.1 Index type base cost
            if (definition.Reduce != null)
                score += AddPenalty(penalties, "MapReduce index (reduce phase)", PenaltyMapReduce);

            bool isJavaScript = definition.Type.IsJavaScript();
            if (isJavaScript)
                score += AddPenalty(penalties, "JavaScript index (Jint interpreter overhead)", PenaltyJavaScript);

            int mapCount = definition.Maps?.Count ?? 0;
            if (mapCount > 1)
            {
                int extraMaps = mapCount - 1;
                score += AddPenalty(penalties, $"Multi-map index ({extraMaps} extra map(s))", PenaltyExtraMap * extraMaps);
            }

            IndexSourceType sourceType = definition.SourceType;
            if (sourceType == IndexSourceType.TimeSeries)
                score += AddPenalty(penalties, "TimeSeries source type", PenaltyTimeSeriesSource);
            else if (sourceType == IndexSourceType.Counters)
                score += AddPenalty(penalties, "Counters source type", PenaltyCountersSource);

            // 1.2 Field penalties
            int fieldCount = 0;
            if (definition.Fields != null)
            {
                foreach (var (fieldName, opts) in definition.Fields)
                {
                    if (opts == null)
                        continue;

                    if (fieldName == Constants.Documents.Indexing.Fields.AllFields)
                        continue; // skip the special @all_fields entry

                    fieldCount++;
                    score += AddPenalty(penalties, $"Field '{fieldName}' (baseline)", PenaltyFieldBase);

                    if (opts.Storage == FieldStorage.Yes)
                        score += AddPenalty(penalties, $"Field '{fieldName}': Storage=Yes", PenaltyFieldStorage);

                    if (opts.Indexing == FieldIndexing.Search)
                        score += AddPenalty(penalties, $"Field '{fieldName}': Indexing=Search (full-text)", PenaltyFieldSearch);

                    if (string.IsNullOrEmpty(opts.Analyzer) == false)
                        score += AddPenalty(penalties, $"Field '{fieldName}': custom Analyzer", PenaltyFieldCustomAnalyzer);

                    if (opts.Suggestions == true)
                        score += AddPenalty(penalties, $"Field '{fieldName}': Suggestions=true (NGram)", PenaltyFieldSuggestions);

                    if (opts.TermVector.HasValue && opts.TermVector.Value != FieldTermVector.No)
                    {
                        if (opts.TermVector.Value == FieldTermVector.WithPositionsAndOffsets)
                            score += AddPenalty(penalties, $"Field '{fieldName}': TermVector=WithPositionsAndOffsets", PenaltyFieldTermVectorPositionsAndOffsets);
                        else
                            score += AddPenalty(penalties, $"Field '{fieldName}': TermVector={opts.TermVector.Value}", PenaltyFieldTermVector);
                    }

                    if (opts.Spatial != null)
                    {
                        score += AddPenalty(penalties, $"Field '{fieldName}': Spatial options (geospatial index)", PenaltyFieldSpatial);
                        if (opts.Spatial.Strategy == SpatialSearchStrategy.QuadPrefixTree)
                            score += AddPenalty(penalties, $"Field '{fieldName}': Spatial Strategy=QuadPrefixTree", PenaltyFieldSpatialQuadTree);
                    }

                    if (opts.Vector != null)
                    {
                        score += AddPenalty(penalties, $"Field '{fieldName}': Vector options (HNSW graph)", PenaltyFieldVector);
                        if (opts.Vector.Dimensions is > 512)
                            score += AddPenalty(penalties, $"Field '{fieldName}': Vector Dimensions > 512", PenaltyFieldVectorHighDimensions);
                    }
                }

                if (fieldCount > HighFieldCountThreshold)
                {
                    int extra = fieldCount - HighFieldCountThreshold;
                    score += AddPenalty(penalties, $"High field count ({fieldCount} fields, {extra} beyond threshold of {HighFieldCountThreshold})", PenaltyHighFieldCountPerExtra * extra);
                }
            }

            // 1.3 Map complexity (Roslyn AST analysis)
            if (!isJavaScript)
            {
                foreach (string map in definition.Maps ?? new HashSet<string>())
                    score += AnalyzeMapExpression(map, penalties);
            }

            // 1.4 Structural penalties
            if (string.IsNullOrEmpty(definition.OutputReduceToCollection) == false)
            {
                score += AddPenalty(penalties, "OutputReduceToCollection (secondary write load)", PenaltyOutputReduceToCollection);
                if (string.IsNullOrEmpty(definition.PatternForOutputReduceToCollectionReferences) == false)
                    score += AddPenalty(penalties, "PatternForOutputReduceToCollectionReferences (extra reference documents)", PenaltyPatternForOutputReduceReferences);
            }

            int additionalSourcesCount = definition.AdditionalSources?.Count ?? 0;
            if (additionalSourcesCount > 0)
                score += AddPenalty(penalties, $"AdditionalSources ({additionalSourcesCount} file(s))", PenaltyAdditionalSourcesBase + PenaltyAdditionalSourcesPerFile * additionalSourcesCount);

            int additionalAssembliesCount = definition.AdditionalAssemblies?.Count ?? 0;
            if (additionalAssembliesCount > 0)
                score += AddPenalty(penalties, $"AdditionalAssemblies ({additionalAssembliesCount} assembly/assemblies)", PenaltyAdditionalAssembliesBase + PenaltyAdditionalAssembliesPerAssembly * additionalAssembliesCount);

            bool indexesAllDocs = collections?.Contains(Constants.Documents.Collections.AllDocumentsCollection, StringComparer.OrdinalIgnoreCase) == true;
            if (indexesAllDocs)
                score += AddPenalty(penalties, "@all_docs collection (indexes every document in the database)", PenaltyAllDocsCollection);

            if (definition.ArchivedDataProcessingBehavior.HasValue &&
                definition.ArchivedDataProcessingBehavior.Value != ArchivedDataProcessingBehavior.ExcludeArchived)
                score += AddPenalty(penalties, $"ArchivedDataProcessingBehavior={definition.ArchivedDataProcessingBehavior.Value}", PenaltyArchivedDataProcessing);

            return score;
        }

        private static int AnalyzeMapExpression(string mapExpression, List<IndexHeavinessPenalty> penalties)
        {
            if (string.IsNullOrWhiteSpace(mapExpression))
                return 0;

            int score = 0;

            try
            {
                var normalizedMap = Static.IndexCompiler.NormalizeFunction(mapExpression);
                var expression = SyntaxFactory.ParseExpression(normalizedMap);

                var visitor = new IndexMapComplexityVisitor();
                visitor.Visit(expression);

                if (visitor.LoadDocumentCount > 0)
                {
                    score += AddPenalty(penalties, "LoadDocument (cascading re-indexing)", PenaltyLoadDocument);
                    if (visitor.LoadDocumentCount > 1)
                    {
                        int extraLoads = visitor.LoadDocumentCount - 1;
                        score += AddPenalty(penalties, $"Multiple LoadDocument calls ({extraLoads} extra)", PenaltyExtraLoadDocument * extraLoads);
                    }
                }

                if (visitor.HasFanout)
                    score += AddPenalty(penalties, "Fanout (SelectMany / multiple from clauses)", PenaltyFanout);

                if (visitor.HasNestedFanout)
                    score += AddPenalty(penalties, "Nested fanout (cartesian product risk)", PenaltyNestedFanout);

                if (visitor.LetClauseCount > LetClauseThreshold)
                {
                    int extra = visitor.LetClauseCount - LetClauseThreshold;
                    score += AddPenalty(penalties, $"Many let clauses ({visitor.LetClauseCount} total, {extra} beyond threshold of {LetClauseThreshold})", PenaltyExtraLetClause * extra);
                }

                if (visitor.HasWhereClause)
                    score += AddPenalty(penalties, "Where clause in map (per-document evaluation)", PenaltyWhere);

                if (visitor.HasRecurse)
                    score += AddPenalty(penalties, "Recurse (unbounded recursive traversal)", PenaltyRecurse);
            }
            catch (InvalidDataException)
            {
                score += AddPenalty(penalties, "Map expression too complex to fully analyze (stack depth exceeded)", PenaltyTooComplexToAnalyze);
            }
            catch
            {
                // Ignore benign parse errors; we simply skip map analysis for that expression
            }

            return score;
        }

        private static double ComputeDataScaleMultiplier(IEnumerable<string> collections, CollectionDataProvider provider)
        {
            if (provider == null)
                return 1.0;

            long totalDocs = 0;
            long totalSizeBytes = 0;
            int collectionCount = 0;

            foreach (string collection in collections ?? Enumerable.Empty<string>())
            {
                (long count, long sizeBytes) = provider(collection);
                totalDocs += count;
                totalSizeBytes += sizeBytes;
                collectionCount++;
            }

            if (collectionCount == 0)
                return 1.0;

            double sizeFactor = GetCollectionSizeFactor(totalDocs);
            double avgDocSizeBytes = totalDocs > 0 ? (double)totalSizeBytes / totalDocs : 0;
            double docSizeFactor = GetDocumentSizeFactor((long)avgDocSizeBytes);

            return sizeFactor * docSizeFactor;
        }

        private static double ComputeRuntimePenalties(IndexStats stats, List<IndexHeavinessPenalty> penalties)
        {
            double score = 0;

            if (stats.MaxNumberOfOutputsPerDocument > 1024)
            {
                score += AddPenalty(penalties, $"MaxNumberOfOutputsPerDocument={stats.MaxNumberOfOutputsPerDocument} (> 1024)", PenaltyMaxOutputsHigh + PenaltyMaxOutputsVeryHigh);
            }
            else if (stats.MaxNumberOfOutputsPerDocument > 100)
            {
                score += AddPenalty(penalties, $"MaxNumberOfOutputsPerDocument={stats.MaxNumberOfOutputsPerDocument} (> 100)", PenaltyMaxOutputsHigh);
            }

            long mapAttempts = (stats.MapAttempts + (stats.MapReferenceAttempts ?? 0));
            long mapErrors = (stats.MapErrors + (stats.MapReferenceErrors ?? 0));
            long reduceAttempts = stats.ReduceAttempts ?? 0;
            long reduceErrors = stats.ReduceErrors ?? 0;
            long totalAttempts = mapAttempts + reduceAttempts;
            long totalErrors = mapErrors + reduceErrors;

            if (totalAttempts > 0)
            {
                double errorRate = (double)totalErrors / totalAttempts;
                if (errorRate > ErrorRateThresholdInvalid)
                    score += AddPenalty(penalties, $"Error rate > {ErrorRateThresholdInvalid * 100:0}% (index likely invalid)", PenaltyErrorRateHigh + PenaltyErrorRateInvalid);
                else if (errorRate > ErrorRateThresholdHigh)
                    score += AddPenalty(penalties, $"Error rate > {ErrorRateThresholdHigh * 100:0}%", PenaltyErrorRateHigh);
            }

            return score;
        }

        private static int AddPenalty(List<IndexHeavinessPenalty> penalties, string reason, int score)
        {
            penalties.Add(new IndexHeavinessPenalty { Reason = reason, Score = score });
            return score;
        }

        private static double AddPenalty(List<IndexHeavinessPenalty> penalties, string reason, double score)
        {
            penalties.Add(new IndexHeavinessPenalty { Reason = reason, Score = score });
            return score;
        }

        private static string GetStaticLabel(int score)
        {
            if (score <= StaticSimpleMax) return "Simple";
            if (score <= StaticModerateMax) return "Moderate";
            if (score <= StaticComplexMax) return "Complex";
            return "Very Complex";
        }

        private static string GetFullLabel(double score)
        {
            if (score <= FullLightMax) return "Light";
            if (score <= FullModerateMax) return "Moderate";
            if (score <= FullHeavyMax) return "Heavy";
            if (score <= FullVeryHeavyMax) return "Very Heavy";
            return "Extreme";
        }

        /// <summary>
        /// Attempts to extract collection names from the map expressions of an IndexDefinition using Roslyn.
        /// Returns null if parsing fails.
        /// </summary>
        internal static HashSet<string> ExtractCollectionsFromMaps(IndexDefinition definition)
        {
            if (definition.Maps == null || definition.Maps.Count == 0)
                return null;

            var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (string map in definition.Maps)
            {
                if (string.IsNullOrWhiteSpace(map))
                    continue;

                try
                {
                    var normalizedMap = Static.IndexCompiler.NormalizeFunction(map);
                    var expression = SyntaxFactory.ParseExpression(normalizedMap);

                    // Try query syntax first, then method syntax
                    var querySyntaxRetriever = CollectionNameRetriever.QuerySyntax;
                    querySyntaxRetriever.Visit(expression);

                    if (querySyntaxRetriever.CollectionNames?.Length > 0)
                    {
                        foreach (string name in querySyntaxRetriever.CollectionNames)
                            collections.Add(name);
                        continue;
                    }

                    var methodSyntaxRetriever = CollectionNameRetriever.MethodSyntax;
                    methodSyntaxRetriever.Visit(expression);

                    if (methodSyntaxRetriever.CollectionNames?.Length > 0)
                    {
                        foreach (string name in methodSyntaxRetriever.CollectionNames)
                            collections.Add(name);
                    }
                    else
                    {
                        // No specific collection found means @all_docs
                        collections.Add(Constants.Documents.Collections.AllDocumentsCollection);
                    }
                }
                catch
                {
                    // Ignore parse errors
                }
            }

            return collections.Count > 0 ? collections : null;
        }

        /// <summary>
        /// Abstraction to retrieve per-collection document count and total size in bytes.
        /// Separates the analyzer from DocumentsStorage to allow testing without a database.
        /// </summary>
        internal delegate (long Count, long TotalSizeBytes) CollectionDataProvider(string collectionName);
    }
}
