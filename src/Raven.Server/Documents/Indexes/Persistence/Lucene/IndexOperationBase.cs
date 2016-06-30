using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

        protected RavenPerFieldAnalyzerWrapper CreateAnalyzer(Func<Analyzer> createDefaultAnalyzer, Dictionary<string, IndexField> fields, bool forQuerying = false)
        {
            Analyzer defaultAnalyzer;
            IndexField value;
            if (fields.TryGetValue(Constants.AllFields, out value) && string.IsNullOrWhiteSpace(value.Analyzer) == false)
                defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value.Analyzer);
            else
                defaultAnalyzer = createDefaultAnalyzer();

            RavenStandardAnalyzer standardAnalyzer = null;
            KeywordAnalyzer keywordAnalyzer = null;
            var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer);
            foreach (var field in fields)
            {
                switch (field.Value.Indexing)
                {
                    case FieldIndexing.NotAnalyzed:
                        if (keywordAnalyzer == null)
                            keywordAnalyzer = new KeywordAnalyzer();

                        perFieldAnalyzerWrapper.AddAnalyzer(field.Key, keywordAnalyzer);
                        break;
                    case FieldIndexing.Analyzed:
                        var analyzer = GetAnalyzer(field.Key, field.Value, forQuerying);
                        if (analyzer != null)
                        {
                            perFieldAnalyzerWrapper.AddAnalyzer(field.Key, analyzer);
                            continue;
                        }

                        if (standardAnalyzer == null)
                            standardAnalyzer = new RavenStandardAnalyzer(global::Lucene.Net.Util.Version.LUCENE_29);

                        perFieldAnalyzerWrapper.AddAnalyzer(field.Key, standardAnalyzer);
                        break;
                }
            }

            return perFieldAnalyzerWrapper;
        }

        public abstract void Dispose();

        private Analyzer GetAnalyzer(string name, IndexField field, bool forQuerying)
        {
            if (string.IsNullOrWhiteSpace(field.Analyzer))
                return null;

            // TODO [ppekrol] can we use one instance like with KeywordAnalyzer and StandardAnalyzer?
            var analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(name, field.Analyzer);

            if (forQuerying)
            {
                var analyzerType = analyzerInstance.GetType();

                var notForQuerying = NotForQuerying
                    .GetOrAdd(analyzerType, t => analyzerInstance.GetType().GetTypeInfo().GetCustomAttributes<NotForQueryingAttribute>(false).Any());

                if (notForQuerying)
                    return null;
            }

            return analyzerInstance;
        }
    }
}