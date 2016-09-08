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
using Lucene.Net.Util;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Server.ServerWide.LowMemoryNotification;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    internal static class IndexedTerms
    {
        private static readonly WeakCache CacheInstance = new WeakCache();

        public class WeakCache : ILowMemoryHandler
        {
            public ConditionalWeakTable<IndexReader, CachedIndexedTerms> TermsCachePerReader = new ConditionalWeakTable<IndexReader, CachedIndexedTerms>();

            public List<WeakReference<IndexReader>> Keys = new List<WeakReference<IndexReader>>();

            private int _count;

            public WeakCache()
            {
                //MemoryStatistics.RegisterLowMemoryHandler(this);
            }

            public void HandleLowMemory()
            {
                //int keysCount = Keys.Count;
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
                //return new LowMemoryHandlerStatistics
                //{
                //    Name = "WeakCache",
                //    Summary = $"Terms cache for {keysCount} keys reader were freed"
                //};
            }

            public void SoftMemoryRelease()
            {
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                return new LowMemoryHandlerStatistics
                {
                    Name = "WeakCache"
                };
            }

            public CachedIndexedTerms GetOrCreateValue(IndexReader reader)
            {
                CachedIndexedTerms value;
                // ReSharper disable once InconsistentlySynchronizedField
                if (TermsCachePerReader.TryGetValue(reader, out value))
                    return value;
                lock (this)
                {
                    if (TermsCachePerReader.TryGetValue(reader, out value))
                        return value;

                    var cachedIndexedTerms = TermsCachePerReader.GetOrCreateValue(reader);
                    Keys.Add(new WeakReference<IndexReader>(reader));
                    if (_count++ % 10 == 0)
                    {
                        IndexReader target;
                        Keys.RemoveAll(x => x.TryGetTarget(out target) == false);
                    }
                    return cachedIndexedTerms;
                }
            }
        }

        public class CachedIndexedTerms : ILowMemoryHandler
        {
            public ConcurrentDictionary<string, FieldCacheInfo> Results = new ConcurrentDictionary<string, FieldCacheInfo>();
            private readonly object _indexName;

            public CachedIndexedTerms(string indexName)
            {
                _indexName = indexName;
                //MemoryStatistics.RegisterLowMemoryHandler(this);
            }

            public void HandleLowMemory()
            {
                //var resultsCount = Results.Count;
                //var info = Results.Values;
                //var termsCnt = info.Sum(fieldCacheInfo => fieldCacheInfo.Results.Count);
                Results.Clear();

                //return new LowMemoryHandlerStatistics
                //{
                //    Name = $"CachedIndexedTerms: {indexName}",
                //    DatabaseName = databaseName,
                //    Summary = $"Cache terms for {indexName} with {resultsCount:#,#} fields with total of {termsCnt:#,#} terms were deleted"
                //};
            }

            public void SoftMemoryRelease()
            {
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                return new LowMemoryHandlerStatistics
                {
                    Name = "CachedIndexedTerms",
                    Metadata = new
                    {
                        IndexName = _indexName
                    },
                    DatabaseName = "CachedIndexedTerms",
                    EstimatedUsedMemory = Results.Sum(x => x.Key.Length * sizeof(char) + x.Value.Results.Sum(y => y.Key.Length * sizeof(char) + y.Value.Length * sizeof(int)))
                };
            }
        }

        public class FieldCacheInfo
        {
            public Dictionary<string, int[]> Results;
            public bool Done;
        }

        public static Dictionary<string, int[]> GetTermsAndDocumentsFor(IndexReader reader, int docBase, string field, string indexName)
        {
            var termsCachePerField = CacheInstance.TermsCachePerReader.GetValue(reader, x => new CachedIndexedTerms(indexName));

            FieldCacheInfo info;
            if (termsCachePerField.Results.TryGetValue(field, out info) && info.Done)
            {
                return info.Results;
            }
            info = termsCachePerField.Results.GetOrAdd(field, new FieldCacheInfo());
            lock (info)
            {
                if (info.Done)
                    return info.Results;
                info.Results = FillCache(reader, docBase, field);
                info.Done = true;
                return info.Results;
            }
        }

        private static Dictionary<string, int[]> FillCache(IndexReader reader, int docBase, string field)
        {
            using (var termDocs = reader.TermDocs())
            {
                var items = new Dictionary<string, int[]>();
                var docsForTerm = new List<int>();

                using (var termEnum = reader.Terms(new Term(field)))
                {
                    do
                    {
                        if (termEnum.Term == null || field != termEnum.Term.Field)
                            break;

                        Term term = termEnum.Term;
                        if (LowPrecisionNumber(term.Field, term.Text))
                            continue;

                        var totalDocCountIncludedDeletes = termEnum.DocFreq();
                        termDocs.Seek(termEnum.Term);
                        while (termDocs.Next() && totalDocCountIncludedDeletes > 0)
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
                    } while (termEnum.Next());
                }
                return items;
            }
        }

        private static bool LowPrecisionNumber(string field, string val)
        {
            if (field.EndsWith("_Range") == false)
                return false;

            if (string.IsNullOrEmpty(val))
                return false;

            return val[0] - NumericUtils.SHIFT_START_INT != 0 &&
                   val[0] - NumericUtils.SHIFT_START_LONG != 0;
        }

        public static RavenJObject[] ReadAllEntriesFromIndex(IndexReader reader)
        {
            if (reader.MaxDoc > 512 * 1024)
            {
                throw new InvalidOperationException("Refusing to extract all index entires from an index with " + reader.MaxDoc +
                                                    " entries, because of the probable time / memory costs associated with that." +
                                                    Environment.NewLine +
                                                    "Viewing Index Entries are a debug tool, and should not be used on indexes of this size. You might want to try Luke, instead.");
            }
            var results = new RavenJObject[reader.MaxDoc];
            using (var termDocs = reader.TermDocs())
            using (var termEnum = reader.Terms())
            {
                while (termEnum.Next())
                {
                    var term = termEnum.Term;
                    if (term == null)
                        break;

                    var text = term.Text;

                    termDocs.Seek(termEnum);
                    for (int i = 0; i < termEnum.DocFreq() && termDocs.Next(); i++)
                    {
                        RavenJObject result = results[termDocs.Doc];
                        if (result == null)
                            results[termDocs.Doc] = result = new RavenJObject();
                        var propertyName = term.Field;
                        if (propertyName.EndsWith("_ConvertToJson") ||
                            propertyName.EndsWith("_IsArray"))
                            continue;
                        if (result.ContainsKey(propertyName))
                        {
                            switch (result[propertyName].Type)
                            {
                                case JTokenType.Array:
                                    ((RavenJArray)result[propertyName]).Add(text);
                                    break;
                                case JTokenType.String:
                                    result[propertyName] = new RavenJArray
                                    {
                                        result[propertyName],
                                        text
                                    };
                                    break;
                                default:
                                    throw new ArgumentException("No idea how to handle " + result[propertyName].Type);
                            }
                        }
                        else
                        {
                            result[propertyName] = text;
                        }
                    }
                }
            }
            return results;
        }
    }
}
