using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Analyzers;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        private static readonly Dictionary<Type, bool> NotForQuerying = new Dictionary<Type, bool>();

        protected RavenPerFieldAnalyzerWrapper CreateAnalyzer(Func<Analyzer> createDefaultAnalyzer, Dictionary<string, IndexField> fields, bool forQuerying = false)
        {
            Analyzer defaultAnalyzer;
            IndexField value;
            if (fields.TryGetValue(Constants.AllFields, out value) && string.IsNullOrWhiteSpace(value.Analyzer) == false)
                defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value.Analyzer);
            else
                defaultAnalyzer = createDefaultAnalyzer();

            StandardAnalyzer standardAnalyzer = null;
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
                        if (string.IsNullOrWhiteSpace(field.Value.Analyzer) == false)
                        {
                            // TODO [ppekrol] can we use one instance like with KeywordAnalyzer and StandardAnalyzer?
                            var analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(field.Key, field.Value.Analyzer);

                            var addAnalyzer = true;
                            if (forQuerying)
                            {
                                var analyzerType = analyzerInstance.GetType();
                                bool notForQuerying;
                                if (NotForQuerying.TryGetValue(analyzerType, out notForQuerying) == false)
                                    NotForQuerying[analyzerType] = notForQuerying = analyzerInstance.GetType().GetTypeInfo().GetCustomAttributes<NotForQueryingAttribute>(false).Any();

                                if (notForQuerying)
                                    addAnalyzer = false;
                            }

                            if (addAnalyzer)
                            {
                                perFieldAnalyzerWrapper.AddAnalyzer(field.Key, analyzerInstance);
                                continue;
                            }
                        }

                        if (standardAnalyzer == null)
                            standardAnalyzer = new StandardAnalyzer(global::Lucene.Net.Util.Version.LUCENE_29);

                        perFieldAnalyzerWrapper.AddAnalyzer(field.Key, standardAnalyzer);
                        break;
                }
            }

            return perFieldAnalyzerWrapper;
        }

        public abstract void Dispose();
    }
}