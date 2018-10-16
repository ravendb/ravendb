// -----------------------------------------------------------------------
//  <copyright file="CachedIndexedTerms.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
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

        public class WeakCache : ILowMemoryHandler
        {
            public ConditionalWeakTable<IndexReader, CachedIndexedTerms> TermsCachePerReader = new ConditionalWeakTable<IndexReader, CachedIndexedTerms>();

            public List<WeakReference<IndexReader>> Keys = new List<WeakReference<IndexReader>>();

            public WeakCache()
            {
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
            }

            public void LowMemory()
            {
                lock (this)
                {
                    foreach (var reference in Keys)
                    {
                        if (reference.TryGetTarget(out IndexReader target))
                            TermsCachePerReader.Remove(target);
                    }

                    Keys.Clear();
                }
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

        public static BlittableJsonReaderObject[] ReadAllEntriesFromIndex(IndexReader reader, JsonOperationContext context, IState state)
        {
            if (reader.MaxDoc > 512 * 1024)
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

        public class CachedIndexedTerms : ILowMemoryHandler
        {
            public readonly ConcurrentDictionary<string, FieldCacheInfo> Results = new ConcurrentDictionary<string, FieldCacheInfo>();

            public CachedIndexedTerms()
            {
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
            }

            public void LowMemory()
            {
                Results.Clear();
            }

            public void LowMemoryOver()
            {
            }
        }

        public class FieldCacheInfo
        {
            public Dictionary<string, int[]> Results;
            public bool Done;
        }
    }
}
