/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Support;

namespace Lucene.Net.Index.Memory
{
    /// <summary>
    /// High-performance single-document main memory Apache Lucene fulltext search index. 
    /// 
    /// <h4>Overview</h4>
    /// 
    /// This class is a replacement/substitute for a large subset of
    /// {@link RAMDirectory} functionality. It is designed to
    /// enable maximum efficiency for on-the-fly matchmaking combining structured and 
    /// fuzzy fulltext search in realtime streaming applications such as Nux XQuery based XML 
    /// message queues, publish-subscribe systems for Blogs/newsfeeds, text chat, data acquisition and 
    /// distribution systems, application level routers, firewalls, classifiers, etc. 
    /// Rather than targeting fulltext search of infrequent queries over huge persistent 
    /// data archives (historic search), this class targets fulltext search of huge 
    /// numbers of queries over comparatively small transient realtime data (prospective 
    /// search). 
    /// For example as in 
    /// <pre>
    /// float score = search(String text, Query query)
    /// </pre>
    /// <p/>
    /// Each instance can hold at most one Lucene "document", with a document containing
    /// zero or more "fields", each field having a name and a fulltext value. The
    /// fulltext value is tokenized (split and transformed) into zero or more index terms 
    /// (aka words) on <c>addField()</c>, according to the policy implemented by an
    /// Analyzer. For example, Lucene analyzers can split on whitespace, normalize to lower case
    /// for case insensitivity, ignore common terms with little discriminatory value such as "he", "in", "and" (stop
    /// words), reduce the terms to their natural linguistic root form such as "fishing"
    /// being reduced to "fish" (stemming), resolve synonyms/inflexions/thesauri 
    /// (upon indexing and/or querying), etc. For details, see
    /// <a target="_blank" href="http://today.java.net/pub/a/today/2003/07/30/LuceneIntro.html">Lucene Analyzer Intro</a>.
    /// <p/>
    /// Arbitrary Lucene queries can be run against this class - see <a target="_blank" 
    /// href="../../../../../../../queryparsersyntax.html">Lucene Query Syntax</a>
    /// as well as <a target="_blank" 
    /// href="http://today.java.net/pub/a/today/2003/11/07/QueryParserRules.html">Query Parser Rules</a>.
    /// Note that a Lucene query selects on the field names and associated (indexed) 
    /// tokenized terms, not on the original fulltext(s) - the latter are not stored 
    /// but rather thrown away immediately after tokenization.
    /// <p/>
    /// For some interesting background information on search technology, see Bob Wyman's
    /// <a target="_blank" 
    /// href="http://bobwyman.pubsub.com/main/2005/05/mary_hodder_poi.html">Prospective Search</a>, 
    /// Jim Gray's
    /// <a target="_blank" href="http://www.acmqueue.org/modules.php?name=Content&amp;pa=showpage&amp;pid=293&amp;page=4">
    /// A Call to Arms - Custom subscriptions</a>, and Tim Bray's
    /// <a target="_blank" 
    /// href="http://www.tbray.org/ongoing/When/200x/2003/07/30/OnSearchTOC">On Search, the Series</a>.
    /// 
    /// 
    /// <h4>Example Usage</h4> 
    /// 
    /// <pre>
    /// Analyzer analyzer = PatternAnalyzer.DEFAULT_ANALYZER;
    /// //Analyzer analyzer = new SimpleAnalyzer();
    /// MemoryIndex index = new MemoryIndex();
    /// index.addField("content", "Readings about Salmons and other select Alaska fishing Manuals", analyzer);
    /// index.addField("author", "Tales of James", analyzer);
    /// QueryParser parser = new QueryParser("content", analyzer);
    /// float score = index.search(parser.parse("+author:james +salmon~ +fish/// manual~"));
    /// if (score &gt; 0.0f) {
    ///     System.out.println("it's a match");
    /// } else {
    ///     System.out.println("no match found");
    /// }
    /// System.out.println("indexData=" + index.toString());
    /// </pre>
    /// 
    /// 
    /// <h4>Example XQuery Usage</h4> 
    /// 
    /// <pre>
    /// (: An XQuery that finds all books authored by James that have something to do with "salmon fishing manuals", sorted by relevance :)
    /// declare namespace lucene = "java:nux.xom.pool.FullTextUtil";
    /// declare variable $query := "+salmon~ +fish/// manual~"; (: any arbitrary Lucene query can go here :)
    /// 
    /// for $book in /books/book[author="James" and lucene:match(abstract, $query) > 0.0]
    /// let $score := lucene:match($book/abstract, $query)
    /// order by $score descending
    /// return $book
    /// </pre>
    /// 
    /// 
    /// <h4>No thread safety guarantees</h4>
    /// 
    /// An instance can be queried multiple times with the same or different queries,
    /// but an instance is not thread-safe. If desired use idioms such as:
    /// <pre>
    /// MemoryIndex index = ...
    /// synchronized (index) {
    ///    // read and/or write index (i.e. add fields and/or query)
    /// } 
    /// </pre>
    /// 
    /// 
    /// <h4>Performance Notes</h4>
    /// 
    /// Internally there's a new data structure geared towards efficient indexing 
    /// and searching, plus the necessary support code to seamlessly plug into the Lucene 
    /// framework.
    /// <p/>
    /// This class performs very well for very small texts (e.g. 10 chars) 
    /// as well as for large texts (e.g. 10 MB) and everything in between. 
    /// Typically, it is about 10-100 times faster than <c>RAMDirectory</c>.
    /// Note that <c>RAMDirectory</c> has particularly 
    /// large efficiency overheads for small to medium sized texts, both in time and space.
    /// Indexing a field with N tokens takes O(N) in the best case, and O(N logN) in the worst 
    /// case. Memory consumption is probably larger than for <c>RAMDirectory</c>.
    /// <p/>
    /// Example throughput of many simple term queries over a single MemoryIndex: 
    /// ~500000 queries/sec on a MacBook Pro, jdk 1.5.0_06, server VM. 
    /// As always, your mileage may vary.
    /// <p/>
    /// If you're curious about
    /// the whereabouts of bottlenecks, run java 1.5 with the non-perturbing '-server
    /// -agentlib:hprof=cpu=samples,depth=10' flags, then study the trace log and
    /// correlate its hotspot trailer with its call stack headers (see <a
    /// target="_blank" href="http://java.sun.com/developer/technicalArticles/Programming/HPROF.html">
    /// hprof tracing </a>).
    ///
    ///</summary>
    [Serializable]
    public partial class MemoryIndex
    {
        /* info for each field: Map<String fieldName, Info field> */
        private HashMap<String, Info> fields = new HashMap<String, Info>();

        /* fields sorted ascending by fieldName; lazily computed on demand */
        [NonSerialized] private KeyValuePair<String, Info>[] sortedFields;

        /* pos: positions[3*i], startOffset: positions[3*i +1], endOffset: positions[3*i +2] */
        private int stride;

        /* Could be made configurable; See {@link Document#setBoost(float)} */
        private static float docBoost = 1.0f;

        private static long serialVersionUID = 2782195016849084649L;

        private static bool DEBUG = false;

        /*
         * Constructs an empty instance.
         */
        public MemoryIndex()
            : this(false)
        {
        }

        /*
         * Constructs an empty instance that can optionally store the start and end
         * character offset of each token term in the text. This can be useful for
         * highlighting of hit locations with the Lucene highlighter package.
         * Private until the highlighter package matures, so that this can actually
         * be meaningfully integrated.
         * 
         * @param storeOffsets
         *            whether or not to store the start and end character offset of
         *            each token term in the text
         */

        private MemoryIndex(bool storeOffsets)
        {
            this.stride = storeOffsets ? 3 : 1;
        }

        /*
         * Convenience method; Tokenizes the given field text and adds the resulting
         * terms to the index; Equivalent to adding an indexed non-keyword Lucene
         * {@link org.apache.lucene.document.Field} that is
         * {@link org.apache.lucene.document.Field.Index#ANALYZED tokenized},
         * {@link org.apache.lucene.document.Field.Store#NO not stored},
         * {@link org.apache.lucene.document.Field.TermVector#WITH_POSITIONS termVectorStored with positions} (or
         * {@link org.apache.lucene.document.Field.TermVector#WITH_POSITIONS termVectorStored with positions and offsets}),
         * 
         * @param fieldName
         *            a name to be associated with the text
         * @param text
         *            the text to tokenize and index.
         * @param analyzer
         *            the analyzer to use for tokenization
         */

        public void AddField(String fieldName, String text, Analyzer analyzer)
        {
            if (fieldName == null)
                throw new ArgumentException("fieldName must not be null");
            if (text == null)
                throw new ArgumentException("text must not be null");
            if (analyzer == null)
                throw new ArgumentException("analyzer must not be null");

            TokenStream stream = analyzer.TokenStream(fieldName, new StringReader(text));

            AddField(fieldName, stream);
        }

        /*
         * Convenience method; Creates and returns a token stream that generates a
         * token for each keyword in the given collection, "as is", without any
         * transforming text analysis. The resulting token stream can be fed into
         * {@link #addField(String, TokenStream)}, perhaps wrapped into another
         * {@link org.apache.lucene.analysis.TokenFilter}, as desired.
         * 
         * @param keywords
         *            the keywords to generate tokens for
         * @return the corresponding token stream
         */

        public TokenStream CreateKeywordTokenStream<T>(ICollection<T> keywords)
        {
            // TODO: deprecate & move this method into AnalyzerUtil?
            if (keywords == null)
                throw new ArgumentException("keywords must not be null");

            return new KeywordTokenStream<T>(keywords);
        }

        /*
         * Equivalent to <c>addField(fieldName, stream, 1.0f)</c>.
         * 
         * @param fieldName
         *            a name to be associated with the text
         * @param stream
         *            the token stream to retrieve tokens from
         */
        public void AddField(String fieldName, TokenStream stream)
        {
            AddField(fieldName, stream, 1.0f);
        }

        /*
         * Iterates over the given token stream and adds the resulting terms to the index;
         * Equivalent to adding a tokenized, indexed, termVectorStored, unstored,
         * Lucene {@link org.apache.lucene.document.Field}.
         * Finally closes the token stream. Note that untokenized keywords can be added with this method via 
         * {@link #CreateKeywordTokenStream(Collection)}, the Lucene contrib <c>KeywordTokenizer</c> or similar utilities.
         * 
         * @param fieldName
         *            a name to be associated with the text
         * @param stream
         *            the token stream to retrieve tokens from.
         * @param boost
         *            the boost factor for hits for this field
         * @see org.apache.lucene.document.Field#setBoost(float)
         */
        public void AddField(String fieldName, TokenStream stream, float boost)
        {
            try
            {
                if (fieldName == null)
                    throw new ArgumentException("fieldName must not be null");
                if (stream == null)
                    throw new ArgumentException("token stream must not be null");
                if (boost <= 0.0f)
                    throw new ArgumentException("boost factor must be greater than 0.0");
                if (fields[fieldName] != null)
                    throw new ArgumentException("field must not be added more than once");

                var terms = new HashMap<String, ArrayIntList>();
                int numTokens = 0;
                int numOverlapTokens = 0;
                int pos = -1;

                var termAtt = stream.AddAttribute<ITermAttribute>();
                var posIncrAttribute = stream.AddAttribute<IPositionIncrementAttribute>();
                var offsetAtt = stream.AddAttribute<IOffsetAttribute>();

                stream.Reset();
                while (stream.IncrementToken())
                {
                    String term = termAtt.Term;
                    if (term.Length == 0) continue; // nothing to do
                    //        if (DEBUG) System.Diagnostics.Debug.WriteLine("token='" + term + "'");
                    numTokens++;
                    int posIncr = posIncrAttribute.PositionIncrement;
                    if (posIncr == 0)
                        numOverlapTokens++;
                    pos += posIncr;

                    ArrayIntList positions = terms[term];
                    if (positions == null)
                    {
                        // term not seen before
                        positions = new ArrayIntList(stride);
                        terms[term] = positions;
                    }
                    if (stride == 1)
                    {
                        positions.Add(pos);
                    }
                    else
                    {
                        positions.Add(pos, offsetAtt.StartOffset, offsetAtt.EndOffset);
                    }
                }
                stream.End();

                // ensure infos.numTokens > 0 invariant; needed for correct operation of terms()
                if (numTokens > 0)
                {
                    boost = boost*docBoost; // see DocumentWriter.addDocument(...)
                    fields[fieldName] = new Info(terms, numTokens, numOverlapTokens, boost);
                    sortedFields = null; // invalidate sorted view, if any
                }
            }
            catch (IOException e)
            {
                // can never happen
                throw new SystemException(string.Empty, e);
            }
            finally
            {
                try
                {
                    if (stream != null) stream.Close();
                }
                catch (IOException e2)
                {
                    throw new SystemException(string.Empty, e2);
                }
            }
        }

        /*
         * Creates and returns a searcher that can be used to execute arbitrary
         * Lucene queries and to collect the resulting query results as hits.
         * 
         * @return a searcher
         */

        public IndexSearcher CreateSearcher()
        {
            MemoryIndexReader reader = new MemoryIndexReader(this);
            IndexSearcher searcher = new IndexSearcher(reader); // ensures no auto-close !!
            reader.SetSearcher(searcher); // to later get hold of searcher.getSimilarity()
            return searcher;
        }

        /*
         * Convenience method that efficiently returns the relevance score by
         * matching this index against the given Lucene query expression.
         * 
         * @param query
         *            an arbitrary Lucene query to run against this index
         * @return the relevance score of the matchmaking; A number in the range
         *         [0.0 .. 1.0], with 0.0 indicating no match. The higher the number
         *         the better the match.
         *
         */

        public float Search(Query query)
        {
            if (query == null)
                throw new ArgumentException("query must not be null");

            Searcher searcher = CreateSearcher();
            try
            {
                float[] scores = new float[1]; // inits to 0.0f (no match)
                searcher.Search(query, new FillingCollector(scores));
                float score = scores[0];
                return score;
            }
            catch (IOException e)
            {
                // can never happen (RAMDirectory)
                throw new SystemException(string.Empty, e);
            }
            finally
            {
                // searcher.close();
                /*
                 * Note that it is harmless and important for good performance to
                 * NOT close the index reader!!! This avoids all sorts of
                 * unnecessary baggage and locking in the Lucene IndexReader
                 * superclass, all of which is completely unnecessary for this main
                 * memory index data structure without thread-safety claims.
                 * 
                 * Wishing IndexReader would be an interface...
                 * 
                 * Actually with the new tight createSearcher() API auto-closing is now
                 * made impossible, hence searcher.close() would be harmless and also 
                 * would not degrade performance...
                 */
            }
        }

        /*
         * Returns a reasonable approximation of the main memory [bytes] consumed by
         * this instance. Useful for smart memory sensititive caches/pools. Assumes
         * fieldNames are interned, whereas tokenized terms are memory-overlaid.
         * 
         * @return the main memory consumption
         */
        public int GetMemorySize()
        {
            // for example usage in a smart cache see nux.xom.pool.Pool    
            int PTR = VM.PTR;
            int INT = VM.INT;
            int size = 0;
            size += VM.SizeOfObject(2*PTR + INT); // memory index
            if (sortedFields != null) size += VM.SizeOfObjectArray(sortedFields.Length);

            size += VM.SizeOfHashMap(fields.Count);
            foreach (var entry in fields)
            {
                // for each Field Info
                Info info = entry.Value;
                size += VM.SizeOfObject(2*INT + 3*PTR); // Info instance vars
                if (info.SortedTerms != null) size += VM.SizeOfObjectArray(info.SortedTerms.Length);

                int len = info.Terms.Count;
                size += VM.SizeOfHashMap(len);

                var iter2 = info.Terms.GetEnumerator();
                while (--len >= 0)
                {
                    iter2.MoveNext();
                    // for each term
                    KeyValuePair<String, ArrayIntList> e = iter2.Current;
                    size += VM.SizeOfObject(PTR + 3*INT); // assumes substring() memory overlay
//        size += STR + 2 * ((String) e.getKey()).length();
                    ArrayIntList positions = e.Value;
                    size += VM.SizeOfArrayIntList(positions.Size());
                }
            }
            return size;
        }

        private int NumPositions(ArrayIntList positions)
        {
            return positions.Size()/stride;
        }

        /* sorts into ascending order (on demand), reusing memory along the way */

        private void SortFields()
        {
            if (sortedFields == null) sortedFields = Sort(fields);
        }

        /* returns a view of the given map's entries, sorted ascending by key */

        private static KeyValuePair<TKey, TValue>[] Sort<TKey, TValue>(HashMap<TKey, TValue> map)
            where TKey : class, IComparable<TKey>
        {
            int size = map.Count;

            var entries = map.ToArray();

            if (size > 1) Array.Sort(entries, TermComparer.KeyComparer);
            return entries;
        }

        /*
         * Returns a String representation of the index data for debugging purposes.
         * 
         * @return the string representation
         */

        public override String ToString()
        {
            StringBuilder result = new StringBuilder(256);
            SortFields();
            int sumChars = 0;
            int sumPositions = 0;
            int sumTerms = 0;

            for (int i = 0; i < sortedFields.Length; i++)
            {
                KeyValuePair<String, Info> entry = sortedFields[i];
                String fieldName = entry.Key;
                Info info = entry.Value;
                info.SortTerms();
                result.Append(fieldName + ":\n");

                int numChars = 0;
                int numPos = 0;
                for (int j = 0; j < info.SortedTerms.Length; j++)
                {
                    KeyValuePair<String, ArrayIntList> e = info.SortedTerms[j];
                    String term = e.Key;
                    ArrayIntList positions = e.Value;
                    result.Append("\t'" + term + "':" + NumPositions(positions) + ":");
                    result.Append(positions.ToString(stride)); // ignore offsets
                    result.Append("\n");
                    numPos += NumPositions(positions);
                    numChars += term.Length;
                }

                result.Append("\tterms=" + info.SortedTerms.Length);
                result.Append(", positions=" + numPos);
                result.Append(", Kchars=" + (numChars/1000.0f));
                result.Append("\n");
                sumPositions += numPos;
                sumChars += numChars;
                sumTerms += info.SortedTerms.Length;
            }

            result.Append("\nfields=" + sortedFields.Length);
            result.Append(", terms=" + sumTerms);
            result.Append(", positions=" + sumPositions);
            result.Append(", Kchars=" + (sumChars/1000.0f));
            return result.ToString();
        }


        ///////////////////////////////////////////////////////////////////////////////
        // Nested classes:
        ///////////////////////////////////////////////////////////////////////////////
        /*
         * Index data structure for a field; Contains the tokenized term texts and
         * their positions.
         */

        [Serializable]
        private sealed class Info
        {
            public static readonly IComparer<KeyValuePair<string, Info>> InfoComparer = new TermComparer<Info>();
            public static readonly IComparer<KeyValuePair<string, ArrayIntList>> ArrayIntListComparer = new TermComparer<ArrayIntList>(); 
            /*
             * Term strings and their positions for this field: Map <String
             * termText, ArrayIntList positions>
             */
            private HashMap<String, ArrayIntList> terms;

            /* Terms sorted ascending by term text; computed on demand */
            [NonSerialized] private KeyValuePair<String, ArrayIntList>[] sortedTerms;

            /* Number of added tokens for this field */
            private int numTokens;

            /* Number of overlapping tokens for this field */
            private int numOverlapTokens;

            /* Boost factor for hits for this field */
            private float boost;

            /* Term for this field's fieldName, lazily computed on demand */
            [NonSerialized] public Term template;

            private static long serialVersionUID = 2882195016849084649L;

            public Info(HashMap<String, ArrayIntList> terms, int numTokens, int numOverlapTokens, float boost)
            {
                this.terms = terms;
                this.numTokens = numTokens;
                this.NumOverlapTokens = numOverlapTokens;
                this.boost = boost;
            }

            public HashMap<string, ArrayIntList> Terms
            {
                get { return terms; }
            }

            public int NumTokens
            {
                get { return numTokens; }
            }

            public int NumOverlapTokens
            {
                get { return numOverlapTokens; }
                set { numOverlapTokens = value; }
            }

            public float Boost
            {
                get { return boost; }
            }

            public KeyValuePair<string, ArrayIntList>[] SortedTerms
            {
                get { return sortedTerms; }
            }

            /*
         * Sorts hashed terms into ascending order, reusing memory along the
         * way. Note that sorting is lazily delayed until required (often it's
         * not required at all). If a sorted view is required then hashing +
         * sort + binary search is still faster and smaller than TreeMap usage
         * (which would be an alternative and somewhat more elegant approach,
         * apart from more sophisticated Tries / prefix trees).
         */

            public void SortTerms()
            {
                if (SortedTerms == null) sortedTerms = Sort(Terms);
            }

            /* note that the frequency can be calculated as numPosition(getPositions(x)) */

            public ArrayIntList GetPositions(String term)
            {
                return Terms[term];
            }

            /* note that the frequency can be calculated as numPosition(getPositions(x)) */

            public ArrayIntList GetPositions(int pos)
            {
                return SortedTerms[pos].Value;
            }
        }


        ///////////////////////////////////////////////////////////////////////////////
        // Nested classes:
        ///////////////////////////////////////////////////////////////////////////////
        /*
         * Efficient resizable auto-expanding list holding <c>int</c> elements;
         * implemented with arrays.
         */

        [Serializable]
        private sealed class ArrayIntList
        {

            private int[] elements;
            private int size = 0;

            private static long serialVersionUID = 2282195016849084649L;

            private ArrayIntList()
                : this(10)
            {

            }

            public ArrayIntList(int initialCapacity)
            {
                elements = new int[initialCapacity];
            }

            public void Add(int elem)
            {
                if (size == elements.Length) EnsureCapacity(size + 1);
                elements[size++] = elem;
            }

            public void Add(int pos, int start, int end)
            {
                if (size + 3 > elements.Length) EnsureCapacity(size + 3);
                elements[size] = pos;
                elements[size + 1] = start;
                elements[size + 2] = end;
                size += 3;
            }

            public int Get(int index)
            {
                if (index >= size) ThrowIndex(index);
                return elements[index];
            }

            public int Size()
            {
                return size;
            }

            public int[] ToArray(int stride)
            {
                int[] arr = new int[Size()/stride];
                if (stride == 1)
                {
                    Array.Copy(elements, 0, arr, 0, size);
                }
                else
                {
                    for (int i = 0, j = 0; j < size; i++, j += stride) arr[i] = elements[j];
                }
                return arr;
            }

            private void EnsureCapacity(int minCapacity)
            {
                int newCapacity = Math.Max(minCapacity, (elements.Length*3)/2 + 1);
                int[] newElements = new int[newCapacity];
                Array.Copy(elements, 0, newElements, 0, size);
                elements = newElements;
            }

            private void ThrowIndex(int index)
            {
                throw new IndexOutOfRangeException("index: " + index
                                                   + ", size: " + size);
            }

            /* returns the first few positions (without offsets); debug only */

            public string ToString(int stride)
            {
                int s = Size()/stride;
                int len = Math.Min(10, s); // avoid printing huge lists
                StringBuilder buf = new StringBuilder(4*len);
                buf.Append("[");
                for (int i = 0; i < len; i++)
                {
                    buf.Append(Get(i*stride));
                    if (i < len - 1) buf.Append(", ");
                }
                if (len != s) buf.Append(", ..."); // and some more...
                buf.Append("]");
                return buf.ToString();
            }
        }


        ///////////////////////////////////////////////////////////////////////////////
        // Nested classes:
        ///////////////////////////////////////////////////////////////////////////////
        private static readonly Term MATCH_ALL_TERM = new Term("");

        /*
         * Search support for Lucene framework integration; implements all methods
         * required by the Lucene IndexReader contracts.
         */

        private sealed partial class MemoryIndexReader : IndexReader
        {
            private readonly MemoryIndex _index;

            private Searcher searcher; // needed to find searcher.getSimilarity() 

            internal MemoryIndexReader(MemoryIndex index)
            {
                _index = index;
            }

            private Info GetInfo(String fieldName)
            {
                return _index.fields[fieldName];
            }

            private Info GetInfo(int pos)
            {
                return _index.sortedFields[pos].Value;
            }

            public override int DocFreq(Term term)
            {
                Info info = GetInfo(term.Field);
                int freq = 0;
                if (info != null) freq = info.GetPositions(term.Text) != null ? 1 : 0;
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.docFreq: " + term + ", freq:" + freq);
                return freq;
            }

            public override TermEnum Terms()
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.terms()");
                return Terms(MATCH_ALL_TERM);
            }

            public override TermEnum Terms(Term term)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.terms: " + term);

                int i; // index into info.sortedTerms
                int j; // index into sortedFields

                _index.SortFields();
                if (_index.sortedFields.Length == 1 && _index.sortedFields[0].Key == term.Field)
                {
                    j = 0; // fast path
                }
                else
                {
                    j = Array.BinarySearch(_index.sortedFields, new KeyValuePair<string, Info>(term.Field, null), Info.InfoComparer);
                }

                if (j < 0)
                {
                    // not found; choose successor
                    j = -j - 1;
                    i = 0;
                    if (j < _index.sortedFields.Length) GetInfo(j).SortTerms();
                }
                else
                {
                    // found
                    Info info = GetInfo(j);
                    info.SortTerms();
                    i = Array.BinarySearch(info.SortedTerms, new KeyValuePair<string, ArrayIntList>(term.Text, null), Info.ArrayIntListComparer);
                    if (i < 0)
                    {
                        // not found; choose successor
                        i = -i - 1;
                        if (i >= info.SortedTerms.Length)
                        {
                            // move to next successor
                            j++;
                            i = 0;
                            if (j < _index.sortedFields.Length) GetInfo(j).SortTerms();
                        }
                    }
                }
                int ix = i;
                int jx = j;

                return new MemoryTermEnum(_index, this, ix, jx);
            }

            public override TermPositions TermPositions()
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.termPositions");

                return new MemoryTermPositions(_index, this);
            }


            public override TermDocs TermDocs()
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.termDocs");
                return TermPositions();
            }

            public override ITermFreqVector[] GetTermFreqVectors(int docNumber)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.getTermFreqVectors");
                // This is okay, ToArray() is as optimized as writing it by hand
                return _index.fields.Keys.Select(k => GetTermFreqVector(docNumber, k)).ToArray();
            }

            public override void GetTermFreqVector(int docNumber, TermVectorMapper mapper)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.getTermFreqVectors");

                //      if (vectors.length == 0) return null;
                foreach (String fieldName in _index.fields.Keys)
                {
                    GetTermFreqVector(docNumber, fieldName, mapper);
                }
            }

            public override void GetTermFreqVector(int docNumber, String field, TermVectorMapper mapper)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.getTermFreqVector");
                Info info = GetInfo(field);
                if (info == null)
                {
                    return;
                }
                info.SortTerms();
                mapper.SetExpectations(field, info.SortedTerms.Length, _index.stride != 1, true);
                for (int i = info.SortedTerms.Length; --i >= 0;)
                {

                    ArrayIntList positions = info.SortedTerms[i].Value;
                    int size = positions.Size();
                    var offsets = new TermVectorOffsetInfo[size/_index.stride];

                    for (int k = 0, j = 1; j < size; k++, j += _index.stride)
                    {
                        int start = positions.Get(j);
                        int end = positions.Get(j + 1);
                        offsets[k] = new TermVectorOffsetInfo(start, end);
                    }
                    mapper.Map(info.SortedTerms[i].Key, _index.NumPositions(info.SortedTerms[i].Value), offsets,
                               (info.SortedTerms[i].Value).ToArray(_index.stride));
                }
            }

            public override ITermFreqVector GetTermFreqVector(int docNumber, String fieldName)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.getTermFreqVector");
                Info info = GetInfo(fieldName);
                if (info == null) return null; // TODO: or return empty vector impl???
                info.SortTerms();

                return new MemoryTermPositionVector(_index, info, fieldName);
            }

            private Similarity GetSimilarity()
            {
                if (searcher != null) return searcher.Similarity;
                return Similarity.Default;
            }

            internal void SetSearcher(Searcher searcher)
            {
                this.searcher = searcher;
            }

            /* performance hack: cache norms to avoid repeated expensive calculations */
            private byte[] cachedNorms;
            private String cachedFieldName;
            private Similarity cachedSimilarity;

            public override byte[] Norms(String fieldName)
            {
                byte[] norms = cachedNorms;
                Similarity sim = GetSimilarity();
                if (fieldName != cachedFieldName || sim != cachedSimilarity)
                {
                    // not cached?
                    Info info = GetInfo(fieldName);
                    int numTokens = info != null ? info.NumTokens : 0;
                    int numOverlapTokens = info != null ? info.NumOverlapTokens : 0;
                    float boost = info != null ? info.Boost : 1.0f;
                    FieldInvertState invertState = new FieldInvertState(0, numTokens, numOverlapTokens, 0, boost);
                    float n = sim.ComputeNorm(fieldName, invertState);
                    byte norm = Similarity.EncodeNorm(n);
                    norms = new byte[] {norm};

                    // cache it for future reuse
                    cachedNorms = norms;
                    cachedFieldName = fieldName;
                    cachedSimilarity = sim;
                    if (DEBUG)
                        System.Diagnostics.Debug.WriteLine("MemoryIndexReader.norms: " + fieldName + ":" + n + ":" +
                                                           norm + ":" + numTokens);
                }
                return norms;
            }

            public override void Norms(String fieldName, byte[] bytes, int offset)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.norms*: " + fieldName);
                byte[] norms = Norms(fieldName);
                Buffer.BlockCopy(norms, 0, bytes, offset, norms.Length);
            }

            protected override void DoSetNorm(int doc, String fieldName, byte value)
            {
                throw new NotSupportedException();
            }

            public override int NumDocs()
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.numDocs");
                return _index.fields.Count > 0 ? 1 : 0;
            }

            public override int MaxDoc
            {
                get
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.maxDoc");
                    return 1;
                }
            }

            public override Document Document(int n)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.document");
                return new Document(); // there are no stored fields
            }

            //When we convert to JDK 1.5 make this Set<String>
            public override Document Document(int n, FieldSelector fieldSelector)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.document");
                return new Document(); // there are no stored fields
            }

            public override bool IsDeleted(int n)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.isDeleted");
                return false;
            }

            public override bool HasDeletions
            {
                get
                {
                    if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.hasDeletions");
                    return false;
                }
            }

            protected override void DoDelete(int docNum)
            {
                throw new NotSupportedException();
            }

            protected override void DoUndeleteAll()
            {
                throw new NotSupportedException();
            }

            protected override void DoCommit(IDictionary<String, String> commitUserData)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.doCommit");

            }

            protected override void DoClose()
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.doClose");
            }

            // lucene >= 1.9 (remove this method for lucene-1.4.3)
            public override ICollection<String> GetFieldNames(FieldOption fieldOption)
            {
                if (DEBUG) System.Diagnostics.Debug.WriteLine("MemoryIndexReader.getFieldNamesOption");
                if (fieldOption == FieldOption.UNINDEXED)
                    return CollectionsHelper<string>.EmptyList();
                if (fieldOption == FieldOption.INDEXED_NO_TERMVECTOR)
                    return CollectionsHelper<string>.EmptyList();
                if (fieldOption == FieldOption.TERMVECTOR_WITH_OFFSET && _index.stride == 1)
                    return CollectionsHelper<string>.EmptyList();
                if (fieldOption == FieldOption.TERMVECTOR_WITH_POSITION_OFFSET && _index.stride == 1)
                    return CollectionsHelper<string>.EmptyList();

                return _index.fields.Keys.AsReadOnly();
            }
        }


        ///////////////////////////////////////////////////////////////////////////////
        // Nested classes:
        ///////////////////////////////////////////////////////////////////////////////
        private static class VM
        {

            public static readonly int PTR = Is64BitVM() ? 8 : 4;

            // bytes occupied by primitive data types
            public static readonly int BOOLEAN = 1;
            public static readonly int BYTE = 1;
            public static readonly int CHAR = 2;
            public static readonly int SHORT = 2;
            public static readonly int INT = 4;
            public static readonly int LONG = 8;
            public static readonly int FLOAT = 4;
            public static readonly int DOUBLE = 8;

            private static readonly int LOG_PTR = (int) Math.Round(Log2(PTR));

            /*
             * Object header of any heap allocated Java object. 
             * ptr to class, info for monitor, gc, hash, etc.
             */
            private static readonly int OBJECT_HEADER = 2*PTR;

            //  assumes n > 0
            //  64 bit VM:
            //    0     --> 0*PTR
            //    1..8  --> 1*PTR
            //    9..16 --> 2*PTR
            private static int SizeOf(int n)
            {
                return (((n - 1) >> LOG_PTR) + 1) << LOG_PTR;
            }

            public static int SizeOfObject(int n)
            {
                return SizeOf(OBJECT_HEADER + n);
            }

            public static int SizeOfObjectArray(int len)
            {
                return SizeOfObject(INT + PTR*len);
            }

            public static int SizeOfCharArray(int len)
            {
                return SizeOfObject(INT + CHAR*len);
            }

            public static int SizeOfIntArray(int len)
            {
                return SizeOfObject(INT + INT*len);
            }

            public static int SizeOfString(int len)
            {
                return SizeOfObject(3*INT + PTR) + SizeOfCharArray(len);
            }

            public static int SizeOfHashMap(int len)
            {
                return SizeOfObject(4*PTR + 4*INT) + SizeOfObjectArray(len)
                       + len*SizeOfObject(3*PTR + INT); // entries
            }

            // note: does not include referenced objects
            public static int SizeOfArrayList(int len)
            {
                return SizeOfObject(PTR + 2*INT) + SizeOfObjectArray(len);
            }

            public static int SizeOfArrayIntList(int len)
            {
                return SizeOfObject(PTR + INT) + SizeOfIntArray(len);
            }

            private static bool Is64BitVM()
            {
                return IntPtr.Size == 8;
            }

            /* logarithm to the base 2. Example: log2(4) == 2, log2(8) == 3 */

            private static double Log2(double value)
            {
                return Math.Log(value, 2);
                //return Math.Log(value) / Math.Log(2);
            }
        }

    }
}
