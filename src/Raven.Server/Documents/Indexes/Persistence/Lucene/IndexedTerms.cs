using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    internal static class IndexedTerms
    {
        private static readonly WeakCache CacheInstance = new WeakCache();

        public sealed class WeakCache : ILowMemoryHandler
        {
            public ConditionalWeakTable<IndexReader, CachedIndexedTerms> TermsCachePerReader = new ConditionalWeakTable<IndexReader, CachedIndexedTerms>();

            public WeakCache()
            {
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
            }

            public void LowMemory(LowMemorySeverity lowMemorySeverity)
            {
                TermsCachePerReader.Clear();
            }

            public void LowMemoryOver()
            {
            }
        }

        public static Dictionary<string, int[]> GetTermsAndDocumentsFor(IndexReader reader, int docBase, string field, string indexName, IState state)
        {
            var termsCachePerField = CacheInstance.TermsCachePerReader.GetValue(reader, x => new CachedIndexedTerms());

            if (termsCachePerField.Results.TryGetValue(field, out FieldCacheInfo info) && info.Done)
                return info.Results;

            info = termsCachePerField.Results.GetOrAdd(field, new FieldCacheInfo());
            if (info.Done)
                return info.Results;

            lock (info)
            {
                if (info.Done)
                    return info.Results;

                info.Results = FillCache(reader, docBase, field, state);
                info.Done = true;

                return info.Results;
            }
        }

        /// <summary>
        /// The terms of <paramref name="field"/> held by each document of the reader, built once from the cached postings
        /// and kept next to them. Returns null when the cached postings do not fit the reader, in which case callers fall
        /// back to intersecting postings.
        /// </summary>
        public static DocumentTermsIndex GetDocumentTermsFor(IndexReader reader, int docBase, string field, string indexName, IState state)
        {
            var termsCachePerField = CacheInstance.TermsCachePerReader.GetValue(reader, x => new CachedIndexedTerms());
            var info = termsCachePerField.Results.GetOrAdd(field, new FieldCacheInfo());

            var documentTerms = Volatile.Read(ref info.DocumentTerms);
            if (documentTerms != null)
                return documentTerms;

            var termsToDocuments = GetTermsAndDocumentsFor(reader, docBase, field, indexName, state);

            lock (info)
            {
                documentTerms = info.DocumentTerms;
                if (documentTerms != null)
                    return documentTerms;

                documentTerms = DocumentTermsIndex.Build(termsToDocuments, docBase, reader.MaxDoc);
                Volatile.Write(ref info.DocumentTerms, documentTerms);
                return documentTerms;
            }
        }

        public static BlittableJsonReaderObject[] ReadAllEntriesFromIndex(IndexReader reader, JsonOperationContext context, bool ignoreLimit, IState state)
        {
            if (reader.MaxDoc > 512 * 1024 && ignoreLimit == false)
            {
                throw new InvalidOperationException($"Refusing to extract all index entries from an index with: {reader.MaxDoc:#,#;;0} " +
                                                    "entries, because of the probable time / memory costs associated with that." +
                                                    Environment.NewLine +
                                                    "Viewing index entries are a debug tool, and should not be used on indexes of this size.");
            }

            var results = new Dictionary<string, object>[reader.MaxDoc];
            using (var termDocs = reader.TermDocs(state))
            using (var termEnum = reader.Terms(state))
            {
                while (termEnum.Next(state))
                {
                    var term = termEnum.Term;
                    if (term == null)
                        break;
                   
                    string text;
                    if (term.Field.EndsWith("__maxX") ||
                        term.Field.EndsWith("__maxY") ||
                        term.Field.EndsWith("__minY") ||
                        term.Field.EndsWith("__minX"))
                    {
                        // This is a Spatial Index field term 
                        // Lucene keeps the index-entries-values for 'Spatial Index Fields' with 'BoundingBox' encoded as 'prefixCoded bytes'
                        // Need to convert to numbers
                        var num = NumericUtils.PrefixCodedToDouble(term.Text);
                        text = NumberUtil.NumberToString(num);
                    }
                    else
                    {
                        text = term.Text;
                    }

                    termDocs.Seek(termEnum, state);
                    for (var i = 0; i < termEnum.DocFreq() && termDocs.Next(state); i++)
                    {
                        var result = results[termDocs.Doc];
                        if (result == null)
                            results[termDocs.Doc] = result = new Dictionary<string, object>();

                        var propertyName = term.Field;
                        if (propertyName.EndsWith(LuceneDocumentConverterBase.ConvertToJsonSuffix) ||
                            propertyName.EndsWith(LuceneDocumentConverterBase.IsArrayFieldSuffix) ||
                            propertyName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
                            continue;


                        if (result.TryGetValue(propertyName, out var oldValue))
                        {
                            if (oldValue is DynamicJsonArray oldValueAsArray)
                            {
                                oldValueAsArray.Add(text);
                                continue;
                            }

                            if (oldValue is string oldValueAsString)
                            {
                                result[propertyName] = oldValueAsArray = new DynamicJsonArray();
                                oldValueAsArray.Add(oldValueAsString);
                                oldValueAsArray.Add(text);
                                continue;
                            }

                            throw new ArgumentException("No idea how to handle " + oldValue.GetType());
                        }

                        result[propertyName] = text;
                    }
                }
            }

            var final = new BlittableJsonReaderObject[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                var doc = new DynamicJsonValue();
                var dictionary = results[i];
                if (dictionary == null)
                    continue;
                foreach (var kvp in dictionary)
                {
                    doc[kvp.Key] = kvp.Value;
                }
                final[i] = context.ReadObject(doc, "index/entries");
            }

            return final;
        }

        private static Dictionary<string, int[]> FillCache(IndexReader reader, int docBase, string field, IState state)
        {
            var items = new Dictionary<string, int[]>();
            var docsForTerm = new List<int>();

            if (string.Equals(field, Constants.Documents.Querying.Facet.AllResults, StringComparison.OrdinalIgnoreCase) == false)
            {
                using (var termDocs = reader.TermDocs(state))
                {

                    using (var termEnum = reader.Terms(new Term(field), state))
                    {
                        do
                        {
                            if (termEnum.Term == null || field != termEnum.Term.Field)
                                break;

                            Term term = termEnum.Term;
                            if (LowPrecisionNumber(term.Field, term.Text))
                                continue;

                            var totalDocCountIncludedDeletes = termEnum.DocFreq();
                            termDocs.Seek(termEnum.Term, state);
                            while (termDocs.Next(state) && totalDocCountIncludedDeletes > 0)
                            {
                                var curDoc = termDocs.Doc;
                                totalDocCountIncludedDeletes -= 1;
                                if (reader.IsDeleted(curDoc))
                                    continue;

                                docsForTerm.Add(curDoc + docBase);
                            }

                            docsForTerm.Sort();
                            items[term.Text] = docsForTerm.ToArray();
                            docsForTerm.Clear();
                        } while (termEnum.Next(state));
                    }
                    return items;
                }
            }

            for (var curDoc = 0; curDoc < reader.MaxDoc; curDoc++)
                docsForTerm.Add(curDoc + docBase);

            items[field] = docsForTerm.ToArray();
            return items;
        }

        private static bool LowPrecisionNumber(string field, string val)
        {
            if (field.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix) == false)
                return false;

            if (string.IsNullOrEmpty(val))
                return false;

            return val[0] - NumericUtils.SHIFT_START_INT != 0 &&
                   val[0] - NumericUtils.SHIFT_START_LONG != 0;
        }

        public sealed class CachedIndexedTerms : ILowMemoryHandler
        {
            public readonly ConcurrentDictionary<string, FieldCacheInfo> Results = new ConcurrentDictionary<string, FieldCacheInfo>();

            public CachedIndexedTerms()
            {
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
            }

            public void LowMemory(LowMemorySeverity lowMemorySeverity)
            {
                Results.Clear();
            }

            public void LowMemoryOver()
            {
            }
        }

        public sealed class FieldCacheInfo
        {
            public Dictionary<string, int[]> Results;
            public bool Done;
            public DocumentTermsIndex DocumentTerms;
        }

        /// <summary>
        /// The inverse of <see cref="FieldCacheInfo.Results"/>: for every document of a reader, the ordinals of the terms
        /// it holds in one field, laid out as offsets into a single array so a document's terms are a slice. Lets a facet
        /// walk a handful of matches instead of intersecting every distinct term's postings with them.
        /// </summary>
        public sealed class DocumentTermsIndex
        {
            public readonly string[] Terms;
            private readonly int[] _offsets;
            private readonly int[] _termOrdinals;

            private DocumentTermsIndex(string[] terms, int[] offsets, int[] termOrdinals)
            {
                Terms = terms;
                _offsets = offsets;
                _termOrdinals = termOrdinals;
            }

            public ReadOnlySpan<int> GetTermOrdinals(int localDoc)
            {
                var start = _offsets[localDoc];
                return new ReadOnlySpan<int>(_termOrdinals, start, _offsets[localDoc + 1] - start);
            }

            public static DocumentTermsIndex Build(Dictionary<string, int[]> termsToDocuments, int docBase, int maxDoc)
            {
                // the cached postings hold global ids, the layout is over reader-local ids
                var offsets = new int[maxDoc + 1];
                var total = 0;
                foreach (var documents in termsToDocuments.Values)
                {
                    foreach (var document in documents)
                    {
                        var localDoc = document - docBase;
                        if ((uint)localDoc >= (uint)maxDoc)
                            return null;

                        offsets[localDoc + 1]++;
                        total++;
                    }
                }

                for (var i = 1; i <= maxDoc; i++)
                    offsets[i] += offsets[i - 1];

                var terms = new string[termsToDocuments.Count];
                var termOrdinals = new int[total];
                var cursors = new int[maxDoc];
                Array.Copy(offsets, cursors, maxDoc);

                var ordinal = 0;
                foreach (var (term, documents) in termsToDocuments)
                {
                    terms[ordinal] = term;
                    foreach (var document in documents)
                        termOrdinals[cursors[document - docBase]++] = ordinal;

                    ordinal++;
                }

                return new DocumentTermsIndex(terms, offsets, termOrdinals);
            }
        }
    }
}