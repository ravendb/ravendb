using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Lucene.Net.Analysis;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static;
using Version = Lucene.Net.Util.Version;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public static class LuceneIndexingHelpers
{
    private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

    public static LuceneRavenPerFieldAnalyzerWrapper CreateLuceneAnalyzer(Index index, IndexDefinitionBaseServerSide indexDefinition, bool forQuerying = false)
    {
        if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
            throw new InvalidOperationException(
                $"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

        var analyzers = new Dictionary<Type, LuceneAnalyzer>();

        var hasDefaultFieldOptions = false;
        LuceneAnalyzer defaultAnalyzerToUse = null;
        LuceneAnalyzer defaultAnalyzer = null;
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
                            defaultAnalyzerToUse = GetLuceneAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying,
                                index.DocumentDatabase.Name);

                        if (defaultAnalyzerToUse == null)
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(Constants.Documents.Indexing.Fields.AllFields,
                                index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);
                        break;

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

        var perFieldAnalyzerWrapper = forQuerying == false && indexDefinition.HasDynamicFields
            ? new LuceneRavenPerFieldAnalyzerWrapper(
                defaultAnalyzerToUse,
                fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer),
                fieldName => GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer))
            : new LuceneRavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse);

        foreach (var field in indexDefinition.IndexFields)
        {
            var fieldName = field.Value.Name;

            switch (field.Value.Indexing)
            {
                case FieldIndexing.Exact:
                    var keywordAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultExactAnalyzerType.Value.Type, CreateKeywordAnalyzer);

                    perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                    break;

                case FieldIndexing.Search:
                    var analyzer = GetLuceneAnalyzer(fieldName, field.Value.Analyzer, analyzers, forQuerying, index.DocumentDatabase.Name);
                    if (analyzer != null)
                    {
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, analyzer);
                        continue;
                    }

                    AddStandardAnalyzer(fieldName);
                    break;

                case FieldIndexing.Default:
                    if (hasDefaultFieldOptions)
                    {
                        // if we have default field options then we need to take into account overrides for regular fields

                        if (defaultAnalyzer == null)
                            defaultAnalyzer = CreateDefaultAnalyzer(fieldName, index.Configuration.DefaultAnalyzerType.Value.Type);

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, defaultAnalyzer);
                        continue;
                    }

                    break;
            }
        }

        return perFieldAnalyzerWrapper;

        void AddStandardAnalyzer(string fieldName)
        {
            var standardAnalyzer = GetOrCreateAnalyzer(fieldName, index.Configuration.DefaultSearchAnalyzerType.Value.Type, CreateStandardAnalyzer);

            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
        }

        LuceneAnalyzer GetOrCreateAnalyzer(string fieldName, Type analyzerType, Func<string, Type, LuceneAnalyzer> createAnalyzer)
        {
            if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
                analyzers[analyzerType] = analyzer = createAnalyzer(fieldName, analyzerType);

            return analyzer;
        }

        LuceneAnalyzer CreateDefaultAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(LowerCaseKeywordAnalyzer))
                return new LowerCaseKeywordAnalyzer();

            return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        LuceneAnalyzer CreateKeywordAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(KeywordAnalyzer))
                return new KeywordAnalyzer();

            return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }

        LuceneAnalyzer CreateStandardAnalyzer(string fieldName, Type analyzerType)
        {
            if (analyzerType == typeof(RavenStandardAnalyzer))
                return new RavenStandardAnalyzer(Version.LUCENE_29);

            return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType);
        }
    }

    private static LuceneAnalyzer GetLuceneAnalyzer(string fieldName, string analyzer, Dictionary<Type, LuceneAnalyzer> analyzers, bool forQuerying, string databaseName)
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
            analyzers[createAnalyzer.Type] = analyzerInstance = createAnalyzer.CreateInstance(fieldName);

        return analyzerInstance;
    }
}
