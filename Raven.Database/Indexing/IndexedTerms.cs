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
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.WindowsRuntime;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
    internal static class IndexedTerms
    {
        private readonly static ConditionalWeakTable<IndexReader, ConcurrentDictionary<string, FieldCacheInfo>> _termsCachePerReader = 
            new ConditionalWeakTable<IndexReader, ConcurrentDictionary<string, FieldCacheInfo>>();

        private class FieldCacheInfo
        {
            public Dictionary<string,List<int>> Results;
            public bool Done;
        }

        public static Dictionary<string, List<int>> GetTermsAndDocumenstFor(IndexReader reader, int docBase, string field)
        {
            var termsCachePerField = _termsCachePerReader.GetOrCreateValue(reader);
            FieldCacheInfo info;
            if (termsCachePerField.TryGetValue(field, out info) && info.Done)
            {
                return info.Results;
            }
            info = termsCachePerField.GetOrAdd(field, new FieldCacheInfo());
            lock (info)
            {
                if (info.Done)
                    return info.Results;
                info.Results = FillCache(reader, docBase, field);
                info.Done = true;
                return info.Results;
            }
        }

        private static Dictionary<string, List<int>> FillCache(IndexReader reader, int docBase, string field)
        {
            using (var termDocs = reader.TermDocs())
            {
                var items = new Dictionary<string, List<int>>();

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
                        var docsForTerm = new List<int>();
                        while (termDocs.Next() && totalDocCountIncludedDeletes > 0)
                        {
                            var curDoc = termDocs.Doc;
                            totalDocCountIncludedDeletes -= 1;
                            if (reader.IsDeleted(curDoc))
                                continue;

                            docsForTerm.Add(curDoc + docBase);
                        }
                        docsForTerm.Sort();
                        items[term.Text] = docsForTerm;
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