using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Corax.Pipeline;
using Lucene.Net.Analysis;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static;
using KeywordTokenizer = Corax.Pipeline.KeywordTokenizer;
using Version = Lucene.Net.Util.Version;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using CoraxAnalyzer = Corax.Analyzers.Analyzer;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Indexes;
using Corax;
using Corax.Fields;
using Constants = Raven.Client.Constants;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static class CoraxIndexingHelpers
{
    private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

    private static CoraxAnalyzer GetCoraxAnalyzer(string fieldName, string analyzer, Dictionary<Type, CoraxAnalyzer> analyzers, bool forQuerying, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(analyzer))
            return null;

        var createAnalyzer = LuceneIndexingExtensions.GetAnalyzerType(fieldName, analyzer, databaseName);

        if (forQuerying)
        {
            var notForQuerying = NotForQuerying
                .GetOrAdd(createAnalyzer.Type, t => t.GetCustomAttributes<NotForQueryingAttribute>(false).Any());

            if (notForQuerying)
                return null;
        }

        if (analyzers.TryGetValue(createAnalyzer.Type, out var analyzerInstance) == false)
            analyzers[createAnalyzer.Type] = analyzerInstance = LuceneAnalyzerAdapter.Create(createAnalyzer.CreateInstance(fieldName));

        return analyzerInstance;
    }

    public static IndexFieldsMapping CreateCoraxAnalyzers(ByteStringContext context, Index index, IndexDefinitionBaseServerSide indexDefinition, bool forQuerying = false)
    {
        if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
            throw new InvalidOperationException(
                $"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

        var analyzers = new Dictionary<Type, CoraxAnalyzer>();
        var hasDefaultFieldOptions = false;
        CoraxAnalyzer defaultAnalyzerToUse = null;
        CoraxAnalyzer defaultAnalyzer = null;
        if (indexDefinition is MapIndexDefinition mid)
        {
            if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
            {
                hasDefaultFieldOptions = true;

                switch (value.Indexing)
                {
                    case FieldIndexing.Exact:
                        defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultExactAnalyzerType.Value.Type,
                            CreateKeywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        if (value.Analyzer != null)
                        {
                            var defaultAnalyzerToUseFromLucene = GetCoraxAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying,
                                index.DocumentDatabase.Name);
                        }

                        if (defaultAnalyzerToUse == null)
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields,
                                index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                        break;

                    case null:
                    case FieldIndexing.No:
                    case FieldIndexing.Default:
                    default:
                        // explicitly ignore all other values
                        break;
                }
            }
        }

        if (defaultAnalyzerToUse == null)
        {
            defaultAnalyzerToUse = defaultAnalyzer =
                CreateDefaultAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultAnalyzerType.Value.Type);
            analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
        }

        var perFieldAnalyzerWrapper = new IndexFieldsMapping(context);
        perFieldAnalyzerWrapper.DefaultAnalyzer = defaultAnalyzerToUse;
        
        foreach (var field in indexDefinition.IndexFields)
        {
            var fieldName = field.Value.Name;
            Slice.From(context, field.Value.Name, out Slice fieldNameSlice);

            var fieldId = field.Value.Id;

            switch (field.Value.Indexing)
            {
                case FieldIndexing.Exact:
                    var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);

                    perFieldAnalyzerWrapper.AddBinding(fieldId, fieldNameSlice, keywordAnalyzer, fieldIndexingMode: FieldIndexingMode.Exact);
                    break;

                case FieldIndexing.Search:
                    var analyzer = GetCoraxAnalyzer(fieldName, field.Value.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);
                    if (analyzer != null)
                    {
                        perFieldAnalyzerWrapper.AddBinding(fieldId, fieldNameSlice, analyzer, fieldIndexingMode: FieldIndexingMode.Search);
                        continue;
                    }

                    var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                    perFieldAnalyzerWrapper.AddBinding(fieldId, fieldNameSlice, standardAnalyzer, fieldIndexingMode: FieldIndexingMode.Search);

                    break;

                case FieldIndexing.Default:
                    if (hasDefaultFieldOptions)
                    {
                        defaultAnalyzer ??= CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type);

                        perFieldAnalyzerWrapper.AddBinding(fieldId, fieldNameSlice, defaultAnalyzer);
                    }

                    break;
            }
        }
        
        return perFieldAnalyzerWrapper;

        CoraxAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, Func<string, Type, CoraxAnalyzer> createAnalyzer)
        {
            if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
            {
                analyzers[analyzerType] = analyzer = createAnalyzer(fieldName, analyzerType);
            }

            return analyzer;
        }

        CoraxAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                return CoraxAnalyzer.Create(default(KeywordTokenizer), default(LowerCaseTransformer));

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));

            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        CoraxAnalyzer CreateKeywordAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(KeywordAnalyzer))
                return CoraxAnalyzer.Create(default(KeywordTokenizer), default(ExactTransformer));

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));
            
            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        CoraxAnalyzer CreateStandardAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(RavenStandardAnalyzer))
                return LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29));    

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));
            
            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }
    }
}
