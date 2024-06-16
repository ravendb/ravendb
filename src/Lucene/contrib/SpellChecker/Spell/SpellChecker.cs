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
    /// String[] suggestions = spellchecker.suggestSimilar("misspelt", 5);
    /// </pre>
    /// 
    /// </summary>
    /// <author>  Nicolas Maisonneuve
    /// </author>
    /// <version>  1.0
    /// </version>
    public class SpellChecker : IDisposable
    {
        /// <summary> Field name for each word in the ngram index.</summary>
        public const System.String F_WORD = "word";
        private readonly Term F_WORD_TERM = new Term(F_WORD);

        /// <summary> the spell index</summary>
        internal Directory spellindex;

        /// <summary> Boost value for start and end grams</summary>
        private const float bStart = 2.0f;
        private const float bEnd = 1.0f;

        // don't use this searcher directly - see #swapSearcher()
        private IndexSearcher searcher;

        /// <summary>
        /// this locks all modifications to the current searcher. 
        /// </summary>
        private static readonly System.Object searcherLock = new System.Object();

        /*
         * this lock synchronizes all possible modifications to the 
         * current index directory. It should not be possible to try modifying
         * the same index concurrently. Note: Do not acquire the searcher lock
         * before acquiring this lock! 
        */
        private static readonly System.Object modifyCurrentIndexLock = new System.Object();
        private volatile bool closed = false;

        internal float minScore = 0.5f;  //LUCENENET-359 Spellchecker accuracy gets overwritten

        private StringDistance sd;

        /// <summary>
        /// Use the given directory as a spell checker index. The directory
        /// is created if it doesn't exist yet.
        /// </summary>
        /// <param name="spellIndex">the spell index directory</param>
        /// <param name="sd">the <see cref="StringDistance"/> measurement to use </param>
        public SpellChecker(Directory spellIndex, StringDistance sd)
        {
            this.SetSpellIndex(spellIndex);
            this.setStringDistance(sd);
        }

        /// <summary>
        /// Use the given directory as a spell checker index with a
        /// <see cref="LevenshteinDistance"/> as the default <see cref="StringDistance"/>. The
        /// directory is created if it doesn't exist yet.
        /// </summary>
        /// <param name="spellIndex">the spell index directory</param>
        public SpellChecker(Directory spellIndex)
            : this(spellIndex, new LevenshteinDistance())
        { }

        /// <summary>
        /// Use a different index as the spell checker index or re-open
        /// the existing index if <c>spellIndex</c> is the same value
        /// as given in the constructor.
        /// </summary>
        /// <param name="spellIndexDir">spellIndexDir the spell directory to use </param>
        /// <throws>AlreadyClosedException if the Spellchecker is already closed</throws>
        /// <throws>IOException if spellchecker can not open the directory</throws>
        virtual public void SetSpellIndex(Directory spellIndexDir)
        {
            // this could be the same directory as the current spellIndex
            // modifications to the directory should be synchronized 
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                if (!IndexReader.IndexExists(spellIndexDir))
                {
                    var writer = new IndexWriter(spellIndexDir, null, true,
                        IndexWriter.MaxFieldLength.UNLIMITED);
                    writer.Close();
                }
                SwapSearcher(spellIndexDir);
            }
        }

        /// <summary>
        /// Sets the <see cref="StringDistance"/> implementation for this
        /// <see cref="SpellChecker"/> instance.
        /// </summary>
        /// <param name="sd">the <see cref="StringDistance"/> implementation for this
        /// <see cref="SpellChecker"/> instance.</param>
        public void setStringDistance(StringDistance sd)
        {
            this.sd = sd;
        }

        /// <summary>
        /// Returns the <see cref="StringDistance"/> instance used by this
        /// <see cref="SpellChecker"/> instance.
        /// </summary>
        /// <returns>
        /// Returns the <see cref="StringDistance"/> instance used by this
        /// <see cref="SpellChecker"/> instance.
        /// </returns>
        public StringDistance GetStringDistance()
        {
            return sd;
        }


        /// <summary>  Set the accuracy 0 &lt; min &lt; 1; default 0.5</summary>
        virtual public void SetAccuracy(float minScore)
        {
            this.minScore = minScore;
        }

        /// <summary> Suggest similar words</summary>
        /// <param name="word">String the word you want a spell check done on
        /// </param>
        /// <param name="num_sug">int the number of suggest words
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> String[]
        /// </returns>
        public virtual System.String[] SuggestSimilar(System.String word, int num_sug)
        {
            return this.SuggestSimilar(word, num_sug, null, null, false);
        }


        /// <summary> Suggest similar words (restricted or not to a field of a user index)</summary>
        /// <param name="word">String the word you want a spell check done on
        /// </param>
        /// <param name="numSug">int the number of suggest words
        /// </param>
        /// <param name="ir">the indexReader of the user index (can be null see field param)
        /// </param>
        /// <param name="field">String the field of the user index: if field is not null, the suggested
        /// words are restricted to the words present in this field.
        /// </param>
        /// <param name="morePopular">boolean return only the suggest words that are more frequent than the searched word
        /// (only if restricted mode = (indexReader!=null and field!=null)
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> String[] the sorted list of the suggest words with this 2 criteria:
        /// first criteria: the edit distance, second criteria (only if restricted mode): the popularity
        /// of the suggest words in the field of the user index
        /// </returns>
        public virtual System.String[] SuggestSimilar(System.String word, int numSug, IndexReader ir, System.String field, bool morePopular)
        {    // obtainSearcher calls ensureOpen
            IndexSearcher indexSearcher = ObtainSearcher();
            try
            {
                float min = this.minScore;
                int lengthWord = word.Length;

                int freq = (ir != null && field != null) ? ir.DocFreq(new Term(field, word)) : 0;
                int goalFreq = (morePopular && ir != null && field != null) ? freq : 0;
                // if the word exists in the real index and we don't care for word frequency, return the word itself
                if (!morePopular && freq > 0)
                {
                    return new String[] { word };
                }

                var query = new BooleanQuery();
                String[] grams;
                String key;

				var alreadySeen = new HashSet<string>();
                for (var ng = GetMin(lengthWord); ng <= GetMax(lengthWord); ng++)
                {
                    key = "gram" + ng; // form key

                    grams = FormGrams(word, ng); // form word into ngrams (allow dups too)

                    if (grams.Length == 0)
                    {
                        continue; // hmm
                    }

                    if (bStart > 0)
                    { // should we boost prefixes?
                        Add(query, "start" + ng, grams[0], bStart); // matches start of word

                    }
                    if (bEnd > 0)
                    { // should we boost suffixes
                        Add(query, "end" + ng, grams[grams.Length - 1], bEnd); // matches end of word

                    }
                    for (int i = 0; i < grams.Length; i++)
                    {
                        Add(query, key, grams[i]);
                    }
                }

                int maxHits = 10 * numSug;

                //    System.out.println("Q: " + query);
                ScoreDoc[] hits = indexSearcher.Search(query, null, maxHits).ScoreDocs;
                //    System.out.println("HITS: " + hits.length());
                SuggestWordQueue sugQueue = new SuggestWordQueue(numSug);

                // go thru more than 'maxr' matches in case the distance filter triggers
                int stop = Math.Min(hits.Length, maxHits);
                SuggestWord sugWord = new SuggestWord();
                for (int i = 0; i < stop; i++)
                {
                    sugWord.termString = indexSearcher.Doc(hits[i].Doc).Get(F_WORD); // get orig word

                    // don't suggest a word for itself, that would be silly
                    if (sugWord.termString.Equals(word))
                    {
                        continue;
                    }

                    // edit distance
                    sugWord.score = sd.GetDistance(word, sugWord.termString);
                    if (sugWord.score < min)
                    {
                        continue;
                    }

                    if (ir != null && field != null)
                    { // use the user index
                        sugWord.freq = ir.DocFreq(new Term(field, sugWord.termString)); // freq in the index
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
                        min = ((SuggestWord)sugQueue.Top()).score;
                    }
                    sugWord = new SuggestWord();
                }

                // convert to array string
                String[] list = new String[sugQueue.Size()];
                for (int i = sugQueue.Size() - 1; i >= 0; i--)
                {
                    list[i] = ((SuggestWord)sugQueue.Pop()).termString;
                }

                return list;
            }
            finally
            {
                ReleaseSearcher(indexSearcher);
            }

        }


        /// <summary> Add a clause to a boolean query.</summary>
        private static void Add(BooleanQuery q, System.String k, System.String v, float boost)
        {
            Query tq = new TermQuery(new Term(k, v));
            tq.Boost = boost;
            q.Add(new BooleanClause(tq, Occur.SHOULD));
        }


        /// <summary> Add a clause to a boolean query.</summary>
        private static void Add(BooleanQuery q, System.String k, System.String v)
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
        private static System.String[] FormGrams(System.String text, int ng)
        {
            int len = text.Length;
            System.String[] res = new System.String[len - ng + 1];
            for (int i = 0; i < len - ng + 1; i++)
            {
                res[i] = text.Substring(i, (i + ng) - (i));
            }
            return res;
        }

        /// <summary>
        /// Removes all terms from the spell check index.
        /// </summary>
        public virtual void ClearIndex()
        {
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                Directory dir = this.spellindex;
                IndexWriter writer = new IndexWriter(dir, null, true, IndexWriter.MaxFieldLength.UNLIMITED);
                writer.Close();
                SwapSearcher(dir);
            }
        }


        /// <summary> Check whether the word exists in the index.</summary>
        /// <param name="word">String
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> true iff the word exists in the index
        /// </returns>
        public virtual bool Exist(System.String word)
        {
            // obtainSearcher calls ensureOpen
            IndexSearcher indexSearcher = ObtainSearcher();
            try
            {
                return indexSearcher.DocFreq(F_WORD_TERM.CreateTerm(word)) > 0;
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
        /// <throws>AlreadyClosedException if the Spellchecker is already closed</throws>
        public virtual void IndexDictionary(IDictionary dict, int mergeFactor, int ramMB)
        {
            lock (modifyCurrentIndexLock)
            {
                EnsureOpen();
                Directory dir = this.spellindex;
                IndexWriter writer = new IndexWriter(spellindex, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
                writer.MergeFactor = mergeFactor;
                writer.SetMaxBufferedDocs(ramMB);

                System.Collections.IEnumerator iter = dict.GetWordsIterator();
                while (iter.MoveNext())
                {
                    System.String word = (System.String)iter.Current;

                    int len = word.Length;
                    if (len < 3)
                    {
                        continue; // too short we bail but "too long" is fine...
                    }

                    if (this.Exist(word))
                    {
                        // if the word already exist in the gramindex
                        continue;
                    }

                    // ok index the word
                    Document doc = CreateDocument(word, GetMin(len), GetMax(len));
                    writer.AddDocument(doc);
                }
                // close writer
                writer.Optimize();
                writer.Close();
                // also re-open the spell index to see our own changes when the next suggestion
                // is fetched:
                SwapSearcher(dir);
            }
        }

        /// <summary>
        /// Indexes the data from the given <see cref="IDictionary"/>.
        /// </summary>
        /// <param name="dict">dict the dictionary to index</param>
        public void IndexDictionary(IDictionary dict)
        {
            IndexDictionary(dict, 300, 10);
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


        private static Document CreateDocument(System.String text, int ng1, int ng2)
        {
            Document doc = new Document();
            doc.Add(new Field(F_WORD, text, Field.Store.YES, Field.Index.NOT_ANALYZED)); // orig term
            AddGram(text, doc, ng1, ng2);
            return doc;
        }


        private static void AddGram(System.String text, Document doc, int ng1, int ng2)
        {
            int len = text.Length;
            for (int ng = ng1; ng <= ng2; ng++)
            {
                System.String key = "gram" + ng;
                System.String end = null;
                for (int i = 0; i < len - ng + 1; i++)
                {
                    System.String gram = text.Substring(i, (i + ng) - (i));
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
                searcher.IndexReader.IncRef();
                return searcher;
            }
        }

        private void ReleaseSearcher(IndexSearcher aSearcher)
        {
            // don't check if open - always decRef 
            // don't decrement the private searcher - could have been swapped
            aSearcher.IndexReader.DecRef();
        }

        private void EnsureOpen()
        {
            if (closed)
            {
                throw new AlreadyClosedException("Spellchecker has been closed");
            }
        }

        public void Close()
        {
            lock (searcherLock)
            {
                EnsureOpen();
                closed = true;
                if (searcher != null)
                {
                    searcher.Close();
                }
                searcher = null;
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
                    indexSearcher.Close();
                    throw new AlreadyClosedException("Spellchecker has been closed");
                }
                if (searcher != null)
                {
                    searcher.Close();
                }
                // set the spellindex in the sync block - ensure consistency.
                searcher = indexSearcher;
                this.spellindex = dir;
            }
        }

        /// <summary>
        /// Creates a new read-only IndexSearcher (for testing purposes)
        /// </summary>
        /// <param name="dir">dir the directory used to open the searcher</param>
        /// <returns>a new read-only IndexSearcher. (throws IOException f there is a low-level IO error)</returns>
        public virtual IndexSearcher CreateSearcher(Directory dir)
        {
            return new IndexSearcher(dir, true);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if the <see cref="SpellChecker"/> is
        /// closed, otherwise <c>false</c>.
        /// </summary>
        /// <returns><c>true</c> if and only if the <see cref="SpellChecker"/> is
        ///         closed, otherwise <c>false</c>.
        ///</returns>
        bool IsClosed()
        {
            return closed;
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

        protected void Dispose(bool disposeOfManagedResources)
        {
            if (disposeOfManagedResources)
            {
                if (!this.closed)
                    this.Close();
            }
        }
    }
}