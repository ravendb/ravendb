/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Index;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using NumericField = Lucene.Net.Documents.NumericField;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using TermDocs = Lucene.Net.Index.TermDocs;
using TermEnum = Lucene.Net.Index.TermEnum;
using FieldCacheSanityChecker = Lucene.Net.Util.FieldCacheSanityChecker;
using Single = Lucene.Net.Support.Single;
using StringHelper = Lucene.Net.Util.StringHelper;

namespace Lucene.Net.Search
{
    
    /// <summary> Expert: The default cache implementation, storing all values in memory.
    /// A WeakDictionary is used for storage.
    /// 
    /// <p/>Created: May 19, 2004 4:40:36 PM
    /// 
    /// </summary>
    /// <since>   lucene 1.4
    /// </since>
    class FieldCacheImpl : FieldCache
    {
        private LongCache _longCache;
        private ByteCache _byteCache;
        private ShortCache _shortCache;
        private FloatCache _floatCache;
        private IntCache _intCache;
        private DoubleCache _doubleCache;
        private StringCache _stringCache;
        private StringIndexCache _stringIndexCache;

        internal FieldCacheImpl()
        {
            Init();
        }

        private void Init()
        {
            _byteCache = new ByteCache(this);
            _shortCache = new ShortCache(this);
            _intCache = new IntCache(this);
            _floatCache = new FloatCache(this);
            _longCache = new LongCache(this);
            _doubleCache = new DoubleCache(this);
            _stringCache = new StringCache(this);
            _stringIndexCache = new StringIndexCache(this);
        }

        public virtual IDisposable PurgeAllCaches()
        {
            lock (this)
            {
                // PurgeAllCaches is replacing the cache with a new one (without actually releasing any memory).
                // When the GC will run, the finalizer of the Segments will be executed and release the unmanaged memory.
                // We'll return the old StringIndexCache and let the caller decide if he wants to dispose it sooner

                var copy = _stringIndexCache;
                Init();
                return copy;
            }
        }

        public void Purge(IndexReader r)
        {
            _byteCache.Purge(r);
            _shortCache.Purge(r);
            _intCache.Purge(r);
            _floatCache.Purge(r);
            _longCache.Purge(r);
            _doubleCache.Purge(r);
            _stringCache.Purge(r);
            _stringIndexCache.Purge(r);
        }

        public virtual CacheEntry[] GetCacheEntries()
        {
            // not sure it has to be Concurrent
            var result = new ConcurrentBag<CacheEntry>();

            _byteCache.AddToBag(result);
            _shortCache.AddToBag(result);
            _intCache.AddToBag(result);
            _floatCache.AddToBag(result);
            _longCache.AddToBag(result);
            _doubleCache.AddToBag(result);
            _stringCache.AddToBag(result);
            _stringIndexCache.AddToBag(result);
           
            return result.ToArray();

        }

        private sealed class CacheEntryImpl : CacheEntry
        {
            private System.Object readerKey;
            private System.String fieldName;
            private System.Type cacheType;
            private System.Object custom;
            private System.Object value;
            internal CacheEntryImpl(System.Object readerKey, System.String fieldName, System.Type cacheType, System.Object custom, System.Object value)
            {
                this.readerKey = readerKey;
                this.fieldName = fieldName;
                this.cacheType = cacheType;
                this.custom = custom;
                this.value = value;
                
                // :HACK: for testing.
                //         if (null != locale || SortField.CUSTOM != sortFieldType) {
                //           throw new RuntimeException("Locale/sortFieldType: " + this);
                //         }
            }

            public override object ReaderKey
            {
                get { return readerKey; }
            }

            public override string FieldName
            {
                get { return fieldName; }
            }

            public override Type CacheType
            {
                get { return cacheType; }
            }

            public override object Custom
            {
                get { return custom; }
            }

            public override object Value
            {
                get { return value; }
            }
        }

        /// <summary> Hack: When thrown from a Parser (NUMERIC_UTILS_* ones), this stops
        /// processing terms and returns the current FieldCache
        /// array.
        /// </summary>

        [Serializable]

        internal sealed class StopFillCacheException:System.SystemException
        {
        }
        
        /// <summary>Expert: Internal cache. </summary>
        internal abstract class Cache<T>
        {
            internal Cache()
            {
                this.wrapper = null;
            }
            
            internal Cache(FieldCache wrapper)
            {
                this.wrapper = wrapper;
            }
            
            internal FieldCache wrapper;

            internal ConditionalWeakTable<object, ConcurrentDictionary<Entry, object>> readerCache = new ConditionalWeakTable<object, ConcurrentDictionary<Entry, object>>();
            
            protected internal abstract T CreateValue(IndexReader reader, Entry key, IState state);

            /* Remove this reader from the cache, if present. */
            public void Purge(IndexReader r)
            {
                object readerKey = r.FieldCacheKey;
                readerCache.Remove(readerKey);
            }
            
            public virtual T Get(IndexReader reader, Entry key, IState state)
            {
                var innerCache = readerCache.GetOrCreateValue(reader.FieldCacheKey);
                var value = innerCache.GetOrAdd(key, new Lazy<T>(() => CreateValue(reader, key, state)));

                if (value is Lazy<T> progress)
                {
                    lock (progress) // we need this lock because create value is expensive, we don't want to perform it several times.
                    {
                        if (progress.IsValueCreated == false)
                        {
                            innerCache[key] = progress.Value;

                            // Only check if key.custom (the parser) is
                            // non-null; else, we check twice for a single
                            // call to FieldCache.getXXX
                            if (key.custom != null && wrapper != null)
                            {
                                System.IO.StreamWriter infoStream = wrapper.InfoStream;
                                if (infoStream != null)
                                {
                                    PrintNewInsanity(infoStream, progress.Value);
                                }
                            }
                        }
                        return progress.Value;
                    }
                }

                return (T) value;
            }

            public void AddToBag(ConcurrentBag<CacheEntry> concurrentBag)
            {
                foreach (var readerCacheEntry in readerCache)
                {
                    var readerKey = readerCacheEntry.Key;
                    if (readerKey == null)
                        continue;

                    var innerCache = readerCacheEntry.Value;
                    foreach (var mapEntry in innerCache)
                    {
                        var entry = mapEntry.Key;
                        var value = mapEntry.Value;
                        
                        concurrentBag.Add(new CacheEntryImpl(readerKey, entry.field, typeof(T), entry.custom,
                            value));
                    }
                }
            }

            private void  PrintNewInsanity(System.IO.StreamWriter infoStream, System.Object value_Renamed)
            {
                FieldCacheSanityChecker.Insanity[] insanities = FieldCacheSanityChecker.CheckSanity(wrapper);
                for (int i = 0; i < insanities.Length; i++)
                {
                    FieldCacheSanityChecker.Insanity insanity = insanities[i];
                    CacheEntry[] entries = insanity.GetCacheEntries();
                    for (int j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Value == value_Renamed)
                        {
                            // OK this insanity involves our entry
                            infoStream.WriteLine("WARNING: new FieldCache insanity created\nDetails: " + insanity.ToString());
                            infoStream.WriteLine("\nStack:\n");
                            infoStream.WriteLine(new System.Exception());
                            break;
                        }
                    }
                }
            }
        }
        
        /// <summary>Expert: Every composite-key in the internal cache is of this type. </summary>
        protected internal class Entry
        {
            internal System.String field; // which Fieldable
            internal System.Object custom; // which custom comparator or parser

            /// <summary>Creates one of these objects for a custom comparator/parser. </summary>
            internal Entry(System.String field, System.Object custom)
            {
                this.field = StringHelper.Intern(field);
                this.custom = custom;
            }
            
            /// <summary>Two of these are equal iff they reference the same field and type. </summary>
            public  override bool Equals(System.Object o)
            {
                if (o is Entry)
                {
                    Entry other = (Entry) o;
                    if (other.field == field)
                    {
                        if (other.custom == null)
                        {
                            if (custom == null)
                                return true;
                        }
                        else if (other.custom.Equals(custom))
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
            
            /// <summary>Composes a hashcode based on the field and type. </summary>
            public override int GetHashCode()
            {
                return field.GetHashCode() ^  (custom == null?0:custom.GetHashCode());
            }
        }
        
        // inherit javadocs
        public virtual sbyte[] GetBytes(IndexReader reader, System.String field, IState state)
        {
            return GetBytes(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual sbyte[] GetBytes(IndexReader reader, System.String field, ByteParser parser, IState state)
        {
            return _byteCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class ByteCache:Cache<sbyte[]>
        {
            internal ByteCache(FieldCache wrapper):base(wrapper)
            {
            }
            protected internal override sbyte[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                ByteParser parser = (ByteParser) entry.custom;
                if (parser == null)
                {
                    return wrapper.GetBytes(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_BYTE_PARSER, state);
                }
                sbyte[] retArray = new sbyte[reader.MaxDoc];
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        sbyte termval = parser.ParseByte(term.Text);
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                return retArray;
            }
        }
        
        
        // inherit javadocs
        public virtual short[] GetShorts(IndexReader reader, System.String field, IState state)
        {
            return GetShorts(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual short[] GetShorts(IndexReader reader, System.String field, ShortParser parser, IState state)
        {
            return _shortCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class ShortCache:Cache<short[]>
        {
            internal ShortCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override short[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                ShortParser parser = (ShortParser) entry.custom;
                if (parser == null)
                {
                    return wrapper.GetShorts(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_SHORT_PARSER, state);
                }
                short[] retArray = new short[reader.MaxDoc];
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        short termval = parser.ParseShort(term.Text);
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                return retArray;
            }
        }
        
        
        // inherit javadocs
        public virtual int[] GetInts(IndexReader reader, System.String field, IState state)
        {
            return GetInts(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual int[] GetInts(IndexReader reader, System.String field, IntParser parser, IState state)
        {
            return _intCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class IntCache:Cache<int[]>
        {
            internal IntCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override int[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                IntParser parser = (IntParser) entry.custom;
                if (parser == null)
                {
                    try
                    {
                        return wrapper.GetInts(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_INT_PARSER, state);
                    }
                    catch (System.FormatException)
                    {
                        return wrapper.GetInts(reader, field, Lucene.Net.Search.FieldCache_Fields.NUMERIC_UTILS_INT_PARSER, state);
                    }
                }
                int[] retArray = null;
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        int termval = parser.ParseInt(term.Text);
                        if (retArray == null)
                        // late init
                            retArray = new int[reader.MaxDoc];
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                if (retArray == null)
                // no values
                    retArray = new int[reader.MaxDoc];
                return retArray;
            }
        }
        
        
        
        // inherit javadocs
        public virtual float[] GetFloats(IndexReader reader, System.String field, IState state)
        {
            return GetFloats(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual float[] GetFloats(IndexReader reader, System.String field, FloatParser parser, IState state)
        {
            
            return _floatCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class FloatCache:Cache<float[]>
        {
            internal FloatCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override float[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                FloatParser parser = (FloatParser) entry.custom;
                if (parser == null)
                {
                    try
                    {
                        return wrapper.GetFloats(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_FLOAT_PARSER, state);
                    }
                    catch (System.FormatException)
                    {
                        return wrapper.GetFloats(reader, field, Lucene.Net.Search.FieldCache_Fields.NUMERIC_UTILS_FLOAT_PARSER, state);
                    }
                }
                float[] retArray = null;
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        float termval = parser.ParseFloat(term.Text);
                        if (retArray == null)
                        // late init
                            retArray = new float[reader.MaxDoc];
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                if (retArray == null)
                // no values
                    retArray = new float[reader.MaxDoc];
                return retArray;
            }
        }
        
        
        
        public virtual long[] GetLongs(IndexReader reader, System.String field, IState state)
        {
            return GetLongs(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual long[] GetLongs(IndexReader reader, System.String field, Lucene.Net.Search.LongParser parser, IState state)
        {
            return _longCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class LongCache:Cache<long[]>
        {
            internal LongCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override long[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                Lucene.Net.Search.LongParser parser = (Lucene.Net.Search.LongParser) entry.custom;
                if (parser == null)
                {
                    try
                    {
                        return wrapper.GetLongs(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_LONG_PARSER, state);
                    }
                    catch (System.FormatException)
                    {
                        return wrapper.GetLongs(reader, field, Lucene.Net.Search.FieldCache_Fields.NUMERIC_UTILS_LONG_PARSER, state);
                    }
                }
                long[] retArray = null;
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        long termval = parser.ParseLong(term.Text);
                        if (retArray == null)
                        // late init
                            retArray = new long[reader.MaxDoc];
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                if (retArray == null)
                // no values
                    retArray = new long[reader.MaxDoc];
                return retArray;
            }
        }
        
        
        // inherit javadocs
        public virtual double[] GetDoubles(IndexReader reader, System.String field, IState state)
        {
            return GetDoubles(reader, field, null, state);
        }
        
        // inherit javadocs
        public virtual double[] GetDoubles(IndexReader reader, System.String field, Lucene.Net.Search.DoubleParser parser, IState state)
        {
            return _doubleCache.Get(reader, new Entry(field, parser), state);
        }
        
        internal sealed class DoubleCache:Cache<double[]>
        {
            internal DoubleCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override double[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                Entry entry = entryKey;
                System.String field = entry.field;
                Lucene.Net.Search.DoubleParser parser = (Lucene.Net.Search.DoubleParser) entry.custom;
                if (parser == null)
                {
                    try
                    {
                        return wrapper.GetDoubles(reader, field, Lucene.Net.Search.FieldCache_Fields.DEFAULT_DOUBLE_PARSER, state);
                    }
                    catch (System.FormatException)
                    {
                        return wrapper.GetDoubles(reader, field, Lucene.Net.Search.FieldCache_Fields.NUMERIC_UTILS_DOUBLE_PARSER, state);
                    }
                }
                double[] retArray = null;
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        double termval = parser.ParseDouble(term.Text);
                        if (retArray == null)
                        // late init
                            retArray = new double[reader.MaxDoc];
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                catch (StopFillCacheException)
                {
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                if (retArray == null)
                // no values
                    retArray = new double[reader.MaxDoc];
                return retArray;
            }
        }
        
        
        // inherit javadocs
        public virtual System.String[] GetStrings(IndexReader reader, System.String field, IState state)
        {
            return _stringCache.Get(reader, new Entry(field, (Parser) null), state);
        }
        
        internal sealed class StringCache:Cache<string[]>
        {
            internal StringCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override string[] CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                System.String field = StringHelper.Intern(entryKey.field);
                System.String[] retArray = new System.String[reader.MaxDoc];
                TermDocs termDocs = reader.TermDocs(state);
                TermEnum termEnum = reader.Terms(new Term(field), state);
                try
                {
                    do 
                    {
                        Term term = termEnum.Term;
                        if (term == null || (System.Object) term.Field != (System.Object) field)
                            break;
                        System.String termval = term.Text;
                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            retArray[termDocs.Doc] = termval;
                        }
                    }
                    while (termEnum.Next(state));
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }
                return retArray;
            }
        }
        
        
        // inherit javadocs
        public virtual StringIndex GetStringIndex(IndexReader reader, System.String field, IState state)
        {
            return _stringIndexCache.Get(reader, new Entry(field, (Parser) null), state);
        }
        
        internal sealed class StringIndexCache : Cache<StringIndex>, IDisposable
        {
            internal StringIndexCache(FieldCache wrapper):base(wrapper)
            {
            }
            
            protected internal override StringIndex CreateValue(IndexReader reader, Entry entryKey, IState state)
            {
                System.String field = StringHelper.Intern(entryKey.field);
                int[] retArray = new int[reader.MaxDoc];
                int[] retArrayOrdered = new int[reader.MaxDoc];
                for (int i = 0; i < retArrayOrdered.Length; i++)
                {
                    retArrayOrdered[i] = -1;
                }

                var length = reader.MaxDoc + 1;
                UnmanagedStringArray mterms = new UnmanagedStringArray(length);
                TermDocs termDocs = reader.TermDocs(state);
                
                SegmentTermEnum termEnum = (SegmentTermEnum)reader.Terms(new Term(field), state);
                int termNumber = 0; // current term number
                int docIndex = 0;
                // an entry for documents that have no terms in this field
                // should a document with no terms be at top or bottom?
                // this puts them at the top - if it is changed, FieldDocSortedHitQueue
                // needs to change as well.
                termNumber++;
                
                try
                {
                    do 
                    {
                        if (termEnum.termBuffer.Field != field || termNumber >= length) break;

                        var canAdd = false;

                        termDocs.Seek(termEnum, state);
                        while (termDocs.Next(state))
                        {
                            canAdd = true;
                            var pt = retArray[termDocs.Doc];
                            retArray[termDocs.Doc] = termNumber;

                            if (pt == 0)
                                retArrayOrdered[docIndex++] = termDocs.Doc;
                        }

                        if (canAdd)
                        {
                            // store the term text only if we don't have deletions
                            mterms.Add(termEnum.termBuffer.TextAsSpan);
                        }
                        else
                        {
                            // the term was deleted but we must preserve the order in the array (it must match the termNumber)
                            mterms.AddDeleted(termEnum.termBuffer);
                        }

                        termNumber++;
                    }
                    while (termEnum.Next(state));
                }
                finally
                {
                    termDocs.Close();
                    termEnum.Close();
                }

                StringIndex value_Renamed = new StringIndex(retArray, retArrayOrdered, mterms);
                return value_Renamed;
            }

            public void Dispose()
            {
                foreach (var keyValue in readerCache)
                {
                    foreach (var keyValuePair in keyValue.Value)
                    {
                        if (keyValuePair.Value is StringIndex si)
                        {
                            si.lookup.Dispose();
                        }
                    }
                }
            }
        }
        
        private volatile System.IO.StreamWriter infoStream;

        public virtual StreamWriter InfoStream
        {
            get { return infoStream; }
            set { infoStream = value; }
        }
    }
}