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

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using Hits = Lucene.Net.Search.Hits;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using TermQuery = Lucene.Net.Search.TermQuery;
using Directory = Lucene.Net.Store.Directory;

namespace SpellChecker.Net.Search.Spell
{
	
	
    /// <summary>  <p>
    /// Spell Checker class  (Main class) <br/>
    /// (initially inspired by the David Spencer code).
    /// </p>
    /// 
    /// <p>Example Usage:
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
    public class SpellChecker
    {
        virtual public void SetSpellIndex(Directory spellindex)
        {
            this.spellindex = spellindex;
        }
        /// <summary>  Set the accuracy 0 &lt; min &lt; 1; default 0.5</summary>
        virtual public void SetAccuracy(float minScore)
        {
            this.minScore = minScore;
        }
		
        /// <summary> Field name for each word in the ngram index.</summary>
        public const System.String F_WORD = "word";
		
		
        /// <summary> the spell index</summary>
        internal Directory spellindex;
		
        /// <summary> Boost value for start and end grams</summary>
        private float bStart = 2.0f;
        private float bEnd = 1.0f;
		
		
        private IndexReader reader;
        internal float minScore = 0.5f;  //LUCENENET-359 Spellchecker accuracy gets overwritten
		
		
        public SpellChecker(Directory gramIndex)
        {
            this.SetSpellIndex(gramIndex);
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
        /// <param name="num_sug">int the number of suggest words
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
        public virtual System.String[] SuggestSimilar(System.String word, int num_sug, IndexReader ir, System.String field, bool morePopular)
        {
            float min = this.minScore;
            TRStringDistance sd = new TRStringDistance(word);
            int lengthWord = word.Length;
			
            int goalFreq = (morePopular && ir != null) ? ir.DocFreq(new Term(field, word)) : 0;
            if (!morePopular && goalFreq > 0)
            {
                return new System.String[]{word}; // return the word if it exist in the index and i don't want a more popular word
            }
			
            BooleanQuery query = new BooleanQuery();
            System.String[] grams;
            System.String key;
			
            for (int ng = GetMin(lengthWord); ng <= GetMax(lengthWord); ng++)
            {
				
                key = "gram" + ng; // form key
				
                grams = FormGrams(word, ng); // form word into ngrams (allow dups too)
				
                if (grams.Length == 0)
                {
                    continue; // hmm
                }
				
                if (bStart > 0)
                {
                    // should we boost prefixes?
                    Add(query, "start" + ng, grams[0], bStart); // matches start of word
                }
                if (bEnd > 0)
                {
                    // should we boost suffixes
                    Add(query, "end" + ng, grams[grams.Length - 1], bEnd); // matches end of word
                }
                for (int i = 0; i < grams.Length; i++)
                {
                    Add(query, key, grams[i]);
                }
            }
			
            IndexSearcher searcher = new IndexSearcher(this.spellindex);
            Hits hits = searcher.Search(query);
            SuggestWordQueue sugqueue = new SuggestWordQueue(num_sug);
			
            int stop = Math.Min(hits.Length(), 10 * num_sug); // go thru more than 'maxr' matches in case the distance filter triggers
            SuggestWord sugword = new SuggestWord();
            for (int i = 0; i < stop; i++)
            {
				
                sugword.string_Renamed = hits.Doc(i).Get(F_WORD); // get orig word)
				
                if (sugword.string_Renamed.Equals(word))
                {
                    continue; // don't suggest a word for itself, that would be silly
                }
				
                //edit distance/normalize with the min word length
                sugword.score = 1.0f - ((float) sd.GetDistance(sugword.string_Renamed) / System.Math.Min(sugword.string_Renamed.Length, lengthWord));
                if (sugword.score < min)
                {
                    continue;
                }
				
                if (ir != null)
                {
                    // use the user index
                    sugword.freq = ir.DocFreq(new Term(field, sugword.string_Renamed)); // freq in the index
                    if ((morePopular && goalFreq > sugword.freq) || sugword.freq < 1)
                    {
                        // don't suggest a word that is not present in the field
                        continue;
                    }
                }
                sugqueue.Insert(sugword);
                if (sugqueue.Size() == num_sug)
                {
                    //if queue full , maintain the min score
                    min = ((SuggestWord) sugqueue.Top()).score;
                }
                sugword = new SuggestWord();
            }
			
            // convert to array string
            System.String[] list = new System.String[sugqueue.Size()];
            for (int i = sugqueue.Size() - 1; i >= 0; i--)
            {
                list[i] = ((SuggestWord) sugqueue.Pop()).string_Renamed;
            }
			
            searcher.Close();
            return list;
        }
		
		
        /// <summary> Add a clause to a boolean query.</summary>
        private static void  Add(BooleanQuery q, System.String k, System.String v, float boost)
        {
            Query tq = new TermQuery(new Term(k, v));
            tq.SetBoost(boost);
            q.Add(new BooleanClause(tq, BooleanClause.Occur.SHOULD));
        }
		
		
        /// <summary> Add a clause to a boolean query.</summary>
        private static void  Add(BooleanQuery q, System.String k, System.String v)
        {
            q.Add(new BooleanClause(new TermQuery(new Term(k, v)), BooleanClause.Occur.SHOULD));
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
		
		
        public virtual void  ClearIndex()
        {
            IndexReader.Unlock(spellindex);
            IndexWriter writer = new IndexWriter(spellindex, null, true);
            writer.Close();
        }
		
		
        /// <summary> Check whether the word exists in the index.</summary>
        /// <param name="word">String
        /// </param>
        /// <throws>  IOException </throws>
        /// <returns> true iff the word exists in the index
        /// </returns>
        public virtual bool Exist(System.String word)
        {
            if (reader == null)
            {
                reader = IndexReader.Open(spellindex);
            }
            return reader.DocFreq(new Term(F_WORD, word)) > 0;
        }
		
		
        /// <summary> Index a Dictionary</summary>
        /// <param name="dict">the dictionary to index
        /// </param>
        /// <throws>  IOException </throws>
        public virtual void  IndexDictionary(Dictionary dict)
        {
            IndexReader.Unlock(spellindex);
            IndexWriter writer = new IndexWriter(spellindex, new WhitespaceAnalyzer(), !IndexReader.IndexExists(spellindex));
            writer.SetMergeFactor(300);
            writer.SetMaxBufferedDocs(150);
			
            System.Collections.IEnumerator iter = dict.GetWordsIterator();
            while (iter.MoveNext())
            {
                System.String word = (System.String) iter.Current;
				
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
			
            // close reader
            reader.Close();
            reader = null;
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
            doc.Add(new Field(F_WORD, text, Field.Store.YES, Field.Index.UN_TOKENIZED)); // orig term
            AddGram(text, doc, ng1, ng2);
            return doc;
        }
		
		
        private static void  AddGram(System.String text, Document doc, int ng1, int ng2)
        {
            int len = text.Length;
            for (int ng = ng1; ng <= ng2; ng++)
            {
                System.String key = "gram" + ng;
                System.String end = null;
                for (int i = 0; i < len - ng + 1; i++)
                {
                    System.String gram = text.Substring(i, (i + ng) - (i));
                    doc.Add(new Field(key, gram, Field.Store.YES, Field.Index.UN_TOKENIZED));
                    if (i == 0)
                    {
                        doc.Add(new Field("start" + ng, gram, Field.Store.YES, Field.Index.UN_TOKENIZED));
                    }
                    end = gram;
                }
                if (end != null)
                {
                    // may not be present if len==ng1
                    doc.Add(new Field("end" + ng, end, Field.Store.YES, Field.Index.UN_TOKENIZED));
                }
            }
        }
		
		
        ~SpellChecker()
        {
            if (reader != null)
            {
                reader.Close();
            }
        }
    }
}
