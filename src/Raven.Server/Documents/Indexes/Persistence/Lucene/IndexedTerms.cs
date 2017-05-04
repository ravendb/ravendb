// -----------------------------------------------------------------------
//  <copyright file="CachedIndexedTerms.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Util;
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
                        IndexReader target;
                        if (reference.TryGetTarget(out target))
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
            var termsCachePerField = CacheInstance.TermsCachePerReader.GetValue(reader, x => new CachedIndexedTerms(indexName));

            FieldCacheInfo info;
            if (termsCachePerField.Results.TryGetValue(field, out info) && info.Done)
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
                throw new InvalidOperationException("Refusing to extract all index entires from an index with " + reader.MaxDoc +
                                                    " entries, because of the probable time / memory costs associated with that." +
                                                    Environment.NewLine +
                                                    "Viewing Index Entries are a debug tool, and should not be used on indexes of this size. You might want to try Luke, instead.");
            }

            var results = new DynamicJsonValue[reader.MaxDoc];
            using (var termDocs = reader.TermDocs(state))
            using (var termEnum = reader.Terms(state))
            {
                while (termEnum.Next(state))
                {
                    var term = termEnum.Term;
                    if (term == null)
                        break;

                    var text = term.Text;

                    termDocs.Seek(termEnum, state);
                    for (var i = 0; i < termEnum.DocFreq() && termDocs.Next(state); i++)
                    {
                        var result = results[termDocs.Doc];
                        if (result == null)
                            results[termDocs.Doc] = result = new DynamicJsonValue();

                        var propertyName = term.Field;
                        if (propertyName.EndsWith("_ConvertToJson") ||
                            propertyName.EndsWith("_IsArray") ||
                            propertyName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
                            continue;

                        var oldValue = result[propertyName];
                        if (oldValue != null)
                        {
                            var oldValueAsArray = oldValue as DynamicJsonArray;
                            if (oldValueAsArray != null)
                            {
                                oldValueAsArray.Add(text);
                                continue;
                            }

                            var oldValueAsString = oldValue as string;
                            if (oldValueAsString != null)
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

            return results
                .Select(x => context.ReadObject(x, "index/entries"))
                .ToArray();
        }

        private static Dictionary<string, int[]> FillCache(IndexReader reader, int docBase, string field, IState state)
        {
            using (var termDocs = reader.TermDocs(state))
            {
                var items = new Dictionary<string, int[]>();
                var docsForTerm = new List<int>();
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
            private readonly object _indexName;

            public CachedIndexedTerms(string indexName)
            {
                _indexName = indexName;
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
