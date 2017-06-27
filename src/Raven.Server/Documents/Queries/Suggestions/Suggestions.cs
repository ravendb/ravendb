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

using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Util;

namespace SpellChecker.Net.Search.Spell
{
    using System;

    using Lucene.Net.Search;
    using Lucene.Net.Store;
    using BooleanClause = Lucene.Net.Search.BooleanClause;
    using BooleanQuery = Lucene.Net.Search.BooleanQuery;
    using Directory = Lucene.Net.Store.Directory;
    using Document = Lucene.Net.Documents.Document;
    using Field = Lucene.Net.Documents.Field;
    using IndexReader = Lucene.Net.Index.IndexReader;
    using IndexSearcher = Lucene.Net.Search.IndexSearcher;
    using IndexWriter = Lucene.Net.Index.IndexWriter;
    using Query = Lucene.Net.Search.Query;
    using Term = Lucene.Net.Index.Term;
    using TermQuery = Lucene.Net.Search.TermQuery;
    using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;


    /// <summary>  <p>
    /// Spell Checker class  (Main class) <br/>
    /// (initially inspired by the David Spencer code).
    /// </p>
    /// 
    /// <p>Example Usage:</p>
    /// 
    /// <pre>
    /// SpellChecker spellchecker = new SpellChecker(spellIndexDirectory);
    /// // To index a field of a user index:
    /// spellchecker.indexDictionary(new LuceneDictionary(my_lucene_reader, a_field));
    /// // To index a file containing words:
    /// spellchecker.indexDictionary(new PlainTextDictionary(new File("myfile.txt")));
    /// string[] suggestions = spellchecker.suggestSimilar("misspelt", 5);
    /// </pre>
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    /// <version>  1.0
    /// </version>
    internal sealed class SpellChecker : IDisposable
    {
        /// <summary> Field name for each word in the ngram index.</summary>
        private const string F_WORD = "word";        
        private readonly Term F_WORD_TERM = new Term(F_WORD);

        /// <summary> the spell index</summary>
        private Directory _index;
        private readonly IState _state;
        
        // don't use this searcher directly - see #swapSearcher()
        private IndexSearcher _searcher;

        private readonly StringDistance _sd;

        /// <summary> Boost value for start and end grams</summary>
        private const float BoostStart = 2.0f;
        private const float BoostEnd = 1.0f;

        /// <summary>
        /// this locks all modifications to the current searcher. 
        /// </summary>
        private static readonly object searcherLock = new object();

        /*
         * this lock synchronizes all possible modifications to the 
         * current index directory. It should not be possible to try modifying
         * the same index concurrently. Note: Do not acquire the searcher lock
         * before acquiring this lock! 
        */
        private static readonly object modifyCurrentIndexLock = new object();        
        private volatile bool closed = false;

        internal float minScore = 0.5f;  //LUCENENET-359 Spell checker accuracy gets overwritten

        /// <summary>
        /// Use the given directory as a spell checker index. The directory
        /// is created if it doesn't exist yet.
        /// </summary>
        /// <param name="spellIndex">the spell index directory</param>
        /// <param name="sd">the <see cref="StringDistance"/> measurement to use </param>
        public SpellChecker(Directory spellIndex, StringDistance sd, IState state)
        {
            this.SetSpellIndex(spellIndex);
            this._sd = sd;
            this._state = state;
        }

        /// <summary>
        /// Use the given directory as a spell checker index with a
        /// <see cref="LevenshteinDistance"/> as the default <see cref="StringDistance"/>. The
        /// directory is created if it doesn't exist yet.
        /// </summary>
        /// <param name="spellIndex">the spell index directory</param>
        public SpellChecker(Directory spellIndex, IState state)
            : this(spellIndex, new LevenshteinDistance(), state )
        { }

        /// <summary>
        /// Use a different index as the spell checker index or re-open
        /// the existing index if <code>spellIndex</code> is the same value
        /// as given in the constructor.
        /// </summary>
        /// <param name="spellIndexDir">spellIndexDir the spell directory to use </param>
        /// <throws>AlreadyClosedException if the spell checker is already closed</throws>
        /// <throws>IOException if spell checker can not open the directory</throws>
        public void SetSpellIndex(Directory spellIndexDir)
        {
            // this could be the same directory as the current spellIndex
            // modifications to the directory should be synchronized 
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                if (!IndexReader.IndexExists(spellIndexDir, _state))
                {
                    var writer = new IndexWriter(spellIndexDir, null, true, IndexWriter.MaxFieldLength.UNLIMITED, _state);
                    writer.Dispose();
                }
                SwapSearcher(spellIndexDir);
            }
        }

        /// <summary>  Set the accuracy 0 &lt; min &lt; 1; default 0.5</summary>
        public void SetAccuracy(float minScore)
        {
            // TODO: Change to property.
            this.minScore = minScore;
        }

        /// <summary> Suggest similar words</summary>
        /// <param name="word">string the word you want a spell check done on
        /// </param>
        /// <param name="num_sug">int the number of suggest words
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> string[]
        /// </returns>
        public string[] SuggestSimilar(string word, int num_sug)
        {
            return this.SuggestSimilar(word, num_sug, null, null, false);
        }


        /// <summary> Suggest similar words (restricted or not to a field of a user index)</summary>
        /// <param name="word">string the word you want a spell check done on
        /// </param>
        /// <param name="numSug">int the number of suggest words
        /// </param>
        /// <param name="ir">the indexReader of the user index (can be null see field param)
        /// </param>
        /// <param name="field">string the field of the user index: if field is not null, the suggested
        /// words are restricted to the words present in this field.
        /// </param>
        /// <param name="morePopular">boolean return only the suggest words that are more frequent than the searched word
        /// (only if restricted mode = (indexReader!=null and field!=null)
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> string[] the sorted list of the suggest words with this 2 criteria:
        /// first criteria: the edit distance, second criteria (only if restricted mode): the popularity
        /// of the suggest words in the field of the user index
        /// </returns>
        public string[] SuggestSimilar(string word, int numSug, IndexReader ir, string field, bool morePopular)
        {    
            // obtainSearcher calls ensureOpen
            IndexSearcher indexSearcher = ObtainSearcher();
            try
            {
                float min = this.minScore;
                int lengthWord = word.Length;

                int freq = (ir != null && field != null) ? ir.DocFreq(new Term(field, word), _state) : 0;
                int goalFreq = (morePopular && ir != null && field != null) ? freq : 0;
                // if the word exists in the real index and we don't care for word frequency, return the word itself
                if (!morePopular && freq > 0)
                {
                    return new string[] { word };
                }

                var query = new BooleanQuery();

                var alreadySeen = new HashSet<string>();
                for (var ng = GetMin(lengthWord); ng <= GetMax(lengthWord); ng++)
                {
                    string key = "gram" + ng;
                    string[] grams = FormGrams(word, ng);

                    if (grams.Length == 0)
                    {
                        continue; // hmm
                    }

                    if (BoostStart > 0)
                    { 
                        // should we boost prefixes?
                        Add(query, "start" + ng, grams[0], BoostStart); // matches start of word
                    }
                    
                    if (BoostEnd > 0)
                    { 
                        // should we boost suffixes
                        Add(query, "end" + ng, grams[grams.Length - 1], BoostEnd); // matches end of word
                    }
                    
                    for (int i = 0; i < grams.Length; i++)
                    {
                        Add(query, key, grams[i]);
                    }
                }

                int maxHits = 10 * numSug;

                //    System.out.println("Q: " + query);
                ScoreDoc[] hits = indexSearcher.Search(query, null, maxHits, _state).ScoreDocs;
                
                //    System.out.println("HITS: " + hits.length());
                var sugQueue = new SuggestWordQueue(numSug);

                // go thru more than 'maxr' matches in case the distance filter triggers
                int stop = Math.Min(hits.Length, maxHits);
                
                var sugWord = new SuggestWord();
                for (int i = 0; i < stop; i++)
                {
                    sugWord.termString = indexSearcher.Doc(hits[i].Doc, _state).Get(F_WORD, _state); // get orig word

                    // don't suggest a word for itself, that would be silly
                    if (sugWord.termString.Equals(word, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    // edit distance
                    sugWord.score = _sd.GetDistance(word, sugWord.termString);
                    if (sugWord.score < min)
                    {
                        continue;
                    }

                    if (ir != null && field != null)
                    { // use the user index
                        sugWord.freq = ir.DocFreq(new Term(field, sugWord.termString), _state); // freq in the index
                        // don't suggest a word that is not present in the field
                        if ((morePopular && goalFreq > sugWord.freq) || sugWord.freq < 1)
                        {
                            continue;
                        }
                    }

                    if (alreadySeen.Add(sugWord.termString) == false) // we already seen this word, no point returning it twice
                        continue;

                    sugQueue.InsertWithOverflow(sugWord);
                    if (sugQueue.Size() == numSug)
                    {
                        // if queue full, maintain the minScore score
                        min = sugQueue.Top().score;
                    }
                    sugWord = new SuggestWord();
                }

                // convert to array string
                string[] list = new string[sugQueue.Size()];
                for (int i = sugQueue.Size() - 1; i >= 0; i--)
                {
                    list[i] = sugQueue.Pop().termString;
                }

                return list;
            }
            finally
            {
                ReleaseSearcher(indexSearcher);
            }

        }


        /// <summary> Add a clause to a boolean query.</summary>
        private static void Add(BooleanQuery q, string k, string v, float boost)
        {
            Query tq = new TermQuery(new Term(k, v));
            tq.Boost = boost;
            q.Add(new BooleanClause(tq, Occur.SHOULD));
        }


        /// <summary> Add a clause to a boolean query.</summary>
        private static void Add(BooleanQuery q, string k, string v)
        {
            q.Add(new BooleanClause(new TermQuery(new Term(k, v)), Occur.SHOULD));
        }


        /// <summary> Form all ngrams for a given word.</summary>
        /// <param name="text">the word to parse
        /// </param>
        /// <param name="ng">the ngram length e.g. 3
        /// </param>
        /// <returns> an array of all ngrams in the word and note that duplicates are not removed
        /// </returns>
        private static string[] FormGrams(string text, int ng)
        {
            int len = text.Length;
            string[] res = new string[len - ng + 1];
            for (int i = 0; i < len - ng + 1; i++)
            {
                res[i] = text.Substring(i, (i + ng) - (i));
            }
            return res;
        }

        /// <summary>
        /// Removes all terms from the spell check index.
        /// </summary>
        public void ClearIndex()
        {
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                Directory dir = this._index;
                IndexWriter writer = new IndexWriter(dir, null, true, IndexWriter.MaxFieldLength.UNLIMITED, _state);
                writer.Dispose();
                SwapSearcher(dir);
            }
        }


        /// <summary> Check whether the word exists in the index.</summary>
        /// <param name="word">string
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> true iff the word exists in the index
        /// </returns>
        public bool Exist(string word)
        {
            // obtainSearcher calls ensureOpen
            IndexSearcher indexSearcher = ObtainSearcher();
            try
            {
                return indexSearcher.DocFreq(F_WORD_TERM.CreateTerm(word), _state) > 0;
            }
            finally
            {
                ReleaseSearcher(indexSearcher);
            }
        }

        /// <summary> Index a Dictionary</summary>
        /// <param name="dict">the dictionary to index</param>
        /// <param name="mergeFactor">mergeFactor to use when indexing</param>
        /// <param name="ramMB">the max amount or memory in MB to use</param>
        /// <throws>  IOException </throws>
        /// <throws>AlreadyClosedException if the spell checker is already closed</throws>
        public void IndexDictionary(IWordsDictionary dict, int mergeFactor, int ramMB, CancellationToken token)
        {
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                Directory dir = this._index;
                IndexWriter writer = new IndexWriter(_index, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED, _state);
                writer.MergeFactor = mergeFactor;
                writer.SetMaxBufferedDocs(ramMB);

                var iter = dict.GetWordsIterator();
                while (iter.MoveNext())
                {
                    token.ThrowIfCancellationRequested();

                    string word = iter.Current;

                    int len = word.Length;
                    if (len < 3)
                    {
                        continue; // too short we bail but "too long" is fine...
                    }

                    if (this.Exist(word))
                    {
                        // if the word already exist in the gram index
                        continue;
                    }

                    // ok index the word
                    Document doc = CreateDocument(word, GetMin(len), GetMax(len));
                    writer.AddDocument(doc, _state);
                }
                // close writer
                writer.Commit(_state);
                writer.Dispose();
                // also re-open the spell index to see our own changes when the next suggestion
                // is fetched:
                SwapSearcher(dir);
            }
        }

        /// <summary>
        /// Indexes the data from the given <see cref="IWordsDictionary"/>.
        /// </summary>
        /// <param name="dict">dict the dictionary to index</param>
        public void IndexDictionary(IWordsDictionary dict, CancellationToken token)
        {
            IndexDictionary(dict, 300, 10, token);
        }

        private int GetMin(int l)
        {
            if (l > 5)
            {
                return 3;
            }
            if (l == 5)
            {
                return 2;
            }
            return 1;
        }


        private int GetMax(int l)
        {
            if (l > 5)
            {
                return 4;
            }
            if (l == 5)
            {
                return 3;
            }
            return 2;
        }


        private static Document CreateDocument(string text, int ng1, int ng2)
        {
            Document doc = new Document();
            doc.Add(new Field(F_WORD, text, Field.Store.YES, Field.Index.NOT_ANALYZED)); // orig term
            AddGram(text, doc, ng1, ng2);
            return doc;
        }


        private static void AddGram(string text, Document doc, int ng1, int ng2)
        {
            int len = text.Length;
            for (int ng = ng1; ng <= ng2; ng++)
            {
                string key = "gram" + ng;
                string end = null;
                for (int i = 0; i < len - ng + 1; i++)
                {
                    string gram = text.Substring(i, (i + ng) - (i));
                    doc.Add(new Field(key, gram, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    if (i == 0)
                    {
                        doc.Add(new Field("start" + ng, gram, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    }
                    end = gram;
                }
                if (end != null)
                {
                    // may not be present if len==ng1
                    doc.Add(new Field("end" + ng, end, Field.Store.NO, Field.Index.NOT_ANALYZED));
                }
            }
        }

        private IndexSearcher ObtainSearcher()
        {
            lock (searcherLock)
            {
                EnsureOpen();
                _searcher.IndexReader.IncRef();
                return _searcher;
            }
        }

        private void ReleaseSearcher(IndexSearcher aSearcher)
        {
            // don't check if open - always decRef 
            // don't decrement the private searcher - could have been swapped
            aSearcher.IndexReader.DecRef(_state);
        }

        private void EnsureOpen()
        {
            if (closed)
            {
                throw new AlreadyClosedException("Spell checker has been closed");
            }
        }

        public void Close()
        {
            lock (searcherLock)
            {
                EnsureOpen();
                
                closed = true;
                
                _searcher?.Dispose();
                _searcher = null;
            }
        }

        private void SwapSearcher(Directory dir)
        {
            /*
             * opening a searcher is possibly very expensive.
             * We rather close it again if the Spellchecker was closed during
             * this operation than block access to the current searcher while opening.
             */
            IndexSearcher indexSearcher = CreateSearcher(dir);
            lock (searcherLock)
            {
                if (closed)
                {
                    indexSearcher.Dispose();
                    throw new AlreadyClosedException("Spell checker has been closed");
                }
                _searcher?.Dispose();
                
                // set the spell index in the sync block - ensure consistency.
                _searcher = indexSearcher;
                
                this._index = dir;
            }
        }

        /// <summary>
        /// Creates a new read-only IndexSearcher (for testing purposes)
        /// </summary>
        /// <param name="dir">dir the directory used to open the searcher</param>
        /// <returns>a new read-only IndexSearcher. (throws IOException f there is a low-level IO error)</returns>
        public IndexSearcher CreateSearcher(Directory dir)
        {
            return new IndexSearcher(dir, true, _state);
        }

        ~SpellChecker()
        {
            this.Dispose(false);
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposeOfManagedResources)
        {
            if (disposeOfManagedResources)
            {
                if (!this.closed)
                    this.Close();
            }
        }
    }

    /// <summary> A simple interface representing a Dictionary</summary>
    internal interface IWordsDictionary
    {
        /// <summary> return all the words present in the dictionary</summary>
        /// <returns> Iterator
        /// </returns>
        IEnumerator<string> GetWordsIterator();
    }
    
    /// <summary>
    /// Interface for string distances.
    /// </summary>
    internal interface StringDistance
    {
        /// <summary>
        /// Returns a float between 0 and 1 based on how similar the specified strings are to one another.  
        /// Returning a value of 1 means the specified strings are identical and 0 means the
        /// string are maximally different.
        /// </summary>
        /// <param name="s1">The first string.</param>
        /// <param name="s2">The second string.</param>
        /// <returns>a float between 0 and 1 based on how similar the specified strings are to one another.</returns>
        float GetDistance(string s1, string s2);
    }

    /// <summary>
    /// Levenshtein edit distance
    /// </summary>
    internal class LevenshteinDistance : StringDistance
    {
        /// <summary>
        /// Returns a float between 0 and 1 based on how similar the specified strings are to one another.  
        /// Returning a value of 1 means the specified strings are identical and 0 means the
        /// string are maximally different.
        /// </summary>
        /// <param name="target">The first string.</param>
        /// <param name="other">The second string.</param>
        /// <returns>a float between 0 and 1 based on how similar the specified strings are to one another.</returns>
        public float GetDistance(string target, string other)
        {
            char[] sa;
            int n;
            int[] p; //'previous' cost array, horizontally
            int[] d; // cost array, horizontally
            int[] _d; //placeholder to assist in swapping p and d

            /*
               The difference between this impl. and the previous is that, rather
               than creating and retaining a matrix of size s.length()+1 by t.length()+1,
               we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
               is the 'current working' distance array that maintains the newest distance cost
               counts as we iterate through the characters of string s.  Each time we increment
               the index of string t we are comparing, d is copied to p, the second int[].  Doing so
               allows us to retain the previous cost counts as required by the algorithm (taking
               the minimum of the cost count to the left, up one, and diagonally up and to the left
               of the current cost count being calculated).  (Note that the arrays aren't really
               copied anymore, just switched...this is clearly much better than cloning an array
               or doing a System.arraycopy() each time  through the outer loop.)

               Effectively, the difference between the two implementations is this one does not
               cause an out of memory condition when calculating the LD over two very large strings.
             */

            sa = target.ToCharArray();
            n = sa.Length;
            p = new int[n + 1];
            d = new int[n + 1];
            int m = other.Length;

            if (n == 0 || m == 0)
            {
                if (n == m)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }


            // indexes into strings s and t
            int i; // iterates through s
            int j; // iterates through t

            char t_j; // jth character of t

            int cost; // cost

            for (i = 0; i <= n; i++)
            {
                p[i] = i;
            }

            for (j = 1; j <= m; j++)
            {
                t_j = other[j - 1];
                d[0] = j;

                for (i = 1; i <= n; i++)
                {
                    cost = sa[i - 1] == t_j ? 0 : 1;
                    // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                    d[i] = Math.Min(Math.Min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
                }

                // copy current distance counts to 'previous row' distance counts
                _d = p;
                p = d;
                d = _d;
            }

            // our last action in the above loop was to switch d and p, so p now
            // actually has the most recent cost counts
            return 1.0f - ((float)p[n] / Math.Max(other.Length, sa.Length));
        }
    }

    /// <summary>  SuggestWord Class, used in suggestSimilar method in SpellChecker class.
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    internal sealed class SuggestWord
    {
        /// <summary> the score of the word</summary>
        public float score;

        /// <summary> The freq of the word</summary>
        public int freq;

        /// <summary> the suggested word</summary>
        public string termString;

        public int CompareTo(SuggestWord a)
        {
            //first criteria: the edit distance
            if (score > a.score)
            {
                return 1;
            }
            if (score < a.score)
            {
                return -1;
            }

            //second criteria (if first criteria is equal): the popularity
            if (freq > a.freq)
            {
                return 1;
            }

            if (freq < a.freq)
            {
                return -1;
            }

            return 0;
        }
    }

    internal sealed class SuggestWordQueue : PriorityQueue<SuggestWord>
    {
        internal SuggestWordQueue(int size)
        {
            Initialize(size);
        }

        public override bool LessThan(SuggestWord a, SuggestWord b)
        {
            var val = a.CompareTo(b);
            return val < 0;
        }
    }
}
