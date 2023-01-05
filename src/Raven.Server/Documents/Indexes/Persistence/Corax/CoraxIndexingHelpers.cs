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
using CoraxAnalyzer = Corax.Analyzer;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Indexes;
using Corax;
using Corax.Mappings;
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

    public static IndexFieldsMapping CreateMappingWithAnalyzers(ByteStringContext context, Index index, IndexDefinitionBaseServerSide indexDefinition, string keyFieldName, bool storedValue, string storedValueFieldName,  bool forQuerying = false)
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
                        defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        if (value.Analyzer != null)
                        {
                            defaultAnalyzerToUse = GetCoraxAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying,
                                index.DocumentDatabase.Name);
                            break;
                        }

                        defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields,
                            index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
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

        
        using var mappingBuilder = forQuerying 
            ? IndexFieldsMappingBuilder.CreateForReader() 
            : IndexFieldsMappingBuilder.CreateForWriter(false);
        mappingBuilder.AddDefaultAnalyzer(defaultAnalyzer ?? defaultAnalyzerToUse);
        
        if (indexDefinition.HasDynamicFields)
        {
            mappingBuilder
                .AddExactAnalyzer(fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer))
                .AddSearchAnalyzer(fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer));
        }
        
        //Adding id of document        
        mappingBuilder.AddBinding(0, keyFieldName, defaultAnalyzer);
        
        foreach (var field in indexDefinition.IndexFields.Values)
        {
            var fieldName = field.Name;
            Slice.From(context, fieldName, out Slice fieldNameSlice);
            var fieldId = field.Id;
            
            if (fieldId == global::Corax.Constants.IndexWriter.DynamicField)
                continue;
            
            var hasSuggestions = field.HasSuggestions;
            var hasSpatial = field.Spatial is not null;
            
            
            switch (field.Indexing)
            {
                case FieldIndexing.Exact:
                    var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, keywordAnalyzer, hasSuggestions, FieldIndexingMode.Exact, hasSpatial);
                    break;

                case FieldIndexing.Search:
                    var analyzer = GetCoraxAnalyzer(fieldName, field.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);
                    if (analyzer != null)
                    {
                        mappingBuilder.AddBinding(fieldId, fieldNameSlice, analyzer, hasSuggestions, FieldIndexingMode.Search, hasSpatial);
                        continue;
                    }

                    var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, standardAnalyzer, hasSuggestions, FieldIndexingMode.Search, hasSpatial);
                    break;

                case FieldIndexing.Default:
                    if (hasDefaultFieldOptions)
                    {
                        defaultAnalyzer ??= CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type);
                    }
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, defaultAnalyzer, hasSuggestions, FieldIndexingMode.Normal, hasSpatial);

                    break;
                case FieldIndexing.No:
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, null, fieldIndexingMode: FieldIndexingMode.No);
                    break;
            }
        }

        if (storedValue)
        {
            mappingBuilder.AddBindingToEnd(storedValueFieldName, fieldIndexingMode: FieldIndexingMode.No);
        }
        
        
        return mappingBuilder.Build();

        CoraxAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, Func<ByteStringContext, string, Type, CoraxAnalyzer> createAnalyzer)
        {
            if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
            {
                analyzers[analyzerType] = analyzer = createAnalyzer(context, fieldName, analyzerType);
            }

            return analyzer;
        }

        CoraxAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                return CoraxAnalyzer.Create(context, default(KeywordTokenizer), default(LowerCaseTransformer));

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));

            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        CoraxAnalyzer CreateKeywordAnalyzer(ByteStringContext context, string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(KeywordAnalyzer))
                return CoraxAnalyzer.Create(context, default(KeywordTokenizer), default(ExactTransformer));

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));
            
            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        CoraxAnalyzer CreateStandardAnalyzer(ByteStringContext context, string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(RavenStandardAnalyzer))
                return LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29));    

            if (analyzerType.IsSubclassOf(typeof(LuceneAnalyzer)))
                return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));
            
            return CoraxIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }
    }
}
