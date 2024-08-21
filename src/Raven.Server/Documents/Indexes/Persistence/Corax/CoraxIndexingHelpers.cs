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
using Corax.Mappings;
using Constants = Raven.Client.Constants;
using Sparrow.Server;
using Sparrow.Threading;
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
        {
            var luceneAnalyzerInstance = createAnalyzer.CreateInstance(fieldName);
            analyzers[createAnalyzer.Type] = analyzerInstance = LuceneAnalyzerAdapter.Create(luceneAnalyzerInstance, forQuerying);
        }

        return analyzerInstance;
    }

    public static IndexFieldsMapping CreateMappingWithAnalyzers(Index index, IndexDefinitionBaseServerSide indexDefinition, string keyFieldName, bool storedValue, string storedValueFieldName,  bool forQuerying = false, bool canContainSourceDocumentId = false)
    {
        using var context = new ByteStringContext(SharedMultipleUseFlag.None);
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
                        defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultExactAnalyzerType.Value.Type, forQuerying, CreateKeywordAnalyzer);
                        break;

                    case FieldIndexing.Search:
                        if (value.Analyzer != null)
                        {
                            defaultAnalyzerToUse = GetCoraxAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying,
                                index.DocumentDatabase.Name);
                            break;
                        }

                        defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields,
                            index.Configuration.DefaultSearchAnalyzerType.Value.Type, forQuerying, CreateStandardAnalyzer);
                        break;
                }
            }
        }

        if (defaultAnalyzerToUse == null)
        {
            defaultAnalyzerToUse = defaultAnalyzer =
                CreateDefaultAnalyzer(Constants.Documents.Indexing.Fields.AllFields, index.Configuration.DefaultAnalyzerType.Value.Type, forQuerying);
            analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
        }

        
        using var mappingBuilder = forQuerying 
            ? IndexFieldsMappingBuilder.CreateForReader() 
            : IndexFieldsMappingBuilder.CreateForWriter(false);
        mappingBuilder.AddDefaultAnalyzer(defaultAnalyzer ?? defaultAnalyzerToUse);
        
        if (indexDefinition.HasDynamicFields)
        {
            mappingBuilder
                .AddExactAnalyzer(fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, forQuerying, CreateKeywordAnalyzer))
                .AddSearchAnalyzer(fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type,forQuerying, CreateStandardAnalyzer));
        }
        
        //Adding id of document        
        mappingBuilder.AddBinding(global::Corax.Constants.IndexWriter.PrimaryKeyFieldId, keyFieldName, defaultAnalyzer);
        
        foreach (var field in indexDefinition.IndexFields.Values)
        {
            var fieldName = field.Name;
            Slice.From(context, fieldName, out Slice fieldNameSlice);
            var fieldId = field.Id;
            
            if (fieldId == global::Corax.Constants.IndexWriter.DynamicField)
                continue;
            
            var hasSuggestions = field.HasSuggestions;
            var hasSpatial = field.Spatial is not null;
            bool shouldStore = field.Storage == FieldStorage.Yes;

            switch (field.Indexing)
            {
                case FieldIndexing.Exact:
                    var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, forQuerying, CreateKeywordAnalyzer);
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, keywordAnalyzer, hasSuggestions, FieldIndexingMode.Exact, shouldStore, hasSpatial);
                    break;

                case FieldIndexing.Search:
                    var analyzer = GetCoraxAnalyzer(fieldName, field.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);
                    if (analyzer != null)
                    {
                        mappingBuilder.AddBinding(fieldId, fieldNameSlice, analyzer, hasSuggestions, FieldIndexingMode.Search, shouldStore, hasSpatial);
                        continue;
                    }

                    var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, forQuerying, CreateStandardAnalyzer);
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, standardAnalyzer, hasSuggestions, FieldIndexingMode.Search, shouldStore, hasSpatial);
                    break;

                case FieldIndexing.Default:
                    if (hasDefaultFieldOptions)
                    {
                        defaultAnalyzer ??= CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type, forQuerying);
                    }
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, defaultAnalyzer, hasSuggestions, FieldIndexingMode.Normal, shouldStore, hasSpatial);

                    break;
                case FieldIndexing.No:
                    mappingBuilder.AddBinding(fieldId, fieldNameSlice, null, fieldIndexingMode: FieldIndexingMode.No, shouldStore: shouldStore);
                    break;
            }
        }

        
        if (canContainSourceDocumentId)
            mappingBuilder.AddBindingToEnd(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, fieldIndexingMode: FieldIndexingMode.Exact);
        
        
        if (storedValue)
        {
            //Warning: This field has to be at the end of known fields. Changing it will require changing behaviour in Projection since we relays it's at the end.
            //See more at: https://github.com/ravendb/ravendb/pull/16157#discussion_r1158259732
            mappingBuilder.AddBindingToEnd(storedValueFieldName, fieldIndexingMode: FieldIndexingMode.No, shouldStore: true);
        }
        
        
        return mappingBuilder.Build();

        CoraxAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, bool isForQuerying, Func<ByteStringContext, string, Type, bool, CoraxAnalyzer> createAnalyzer)
        {
            if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
            {
                analyzers[analyzerType] = analyzer = createAnalyzer(context, fieldName, analyzerType, isForQuerying);
            }

            return analyzer;
        }

        CoraxAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType, bool isForQuerying)
        {
            if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                return CoraxAnalyzer.Create(context, default(KeywordTokenizer), default(LowerCaseTransformer));

            return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType), isForQuerying);
        }

        CoraxAnalyzer CreateKeywordAnalyzer(ByteStringContext context, string fieldName, Type analyzerType, bool isForQuerying)
        {
            if (analyzerType == typeof(KeywordAnalyzer))
                return CoraxAnalyzer.Create(context, default(KeywordTokenizer), default(ExactTransformer));

            return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType), isForQuerying);
        }

        CoraxAnalyzer CreateStandardAnalyzer(ByteStringContext context, string fieldName, Type analyzerType, bool forQuerying)
        {
            if (analyzerType == typeof(RavenStandardAnalyzer))
                return LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29), forQuerying);    
            
            return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType), forQuerying);
        }
    }
}
