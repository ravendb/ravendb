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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Sparrow.Json;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries.MoreLikeThis
{
    /// <summary> Generate "more like this" similarity queries. 
    /// Based on this mail:
    /// <pre>
    /// Lucene does let you access the document frequency of terms, with IndexReader.DocFreq().
    /// Term frequencies can be computed by re-tokenizing the text, which, for a single document,
    /// is usually fast enough.  But looking up the DocFreq() of every term in the document is
    /// probably too slow.
    /// 
    /// You can use some heuristics to prune the set of terms, to avoid calling DocFreq() too much,
    /// or at all.  Since you're trying to maximize a tf*idf score, you're probably most interested
    /// in terms with a high tf. Choosing a tf threshold even as low as two or three will radically
    /// reduce the number of terms under consideration.  Another heuristic is that terms with a
    /// high idf (i.e., a low df) tend to be longer.  So you could threshold the terms by the
    /// number of characters, not selecting anything less than, e.g., six or seven characters.
    /// With these sorts of heuristics you can usually find small set of, e.g., ten or fewer terms
    /// that do a pretty good job of characterizing a document.
    /// 
    /// It all depends on what you're trying to do.  If you're trying to eek out that last percent
    /// of precision and recall regardless of computational difficulty so that you can win a TREC
    /// competition, then the techniques I mention above are useless.  But if you're trying to
    /// provide a "more like this" button on a search results page that does a decent job and has
    /// good performance, such techniques might be useful.
    /// 
    /// An efficient, effective "more-like-this" query generator would be a great contribution, if
    /// anyone's interested.  I'd imagine that it would take a Reader or a String (the document's
    /// text), analyzer Analyzer, and return a set of representative terms using heuristics like those
    /// above.  The frequency and length thresholds could be parameters, etc.
    /// 
    /// Doug
    /// </pre>
    /// 
    /// <h3>Initial Usage</h3>
    /// 
    /// This class has lots of options to try to make it efficient and flexible.
    /// See the body of <see cref="Main"/> below in the source for real code, or
    /// if you want pseudo code, the simplest possible usage is as follows. The bold
    /// fragment is specific to this class.
    /// 
    /// <pre>
    /// 
    /// IndexReader ir = ...
    /// IndexSearcher is = ...
    /// <b>
    /// MoreLikeThis mlt = new MoreLikeThis(ir);
    /// Reader target = ... </b><em>// orig source of doc you want to find similarities to</em><b>
    /// Query query = mlt.Like( target);
    /// </b>
    /// Hits hits = is.Search(query);
    /// <em>// now the usual iteration thru 'hits' - the only thing to watch for is to make sure
    /// you ignore the doc if it matches your 'target' document, as it should be similar to itself </em>
    /// 
    /// </pre>
    /// 
    /// Thus you:
    /// <ol>
    /// <li> do your normal, Lucene setup for searching,</li>
    /// <li> create a MoreLikeThis,</li>
    /// <li> get the text of the doc you want to find similarities to</li>
    /// <li> then call one of the Like() calls to generate a similarity query</li>
    /// <li> call the searcher to find the similar docs</li>
    /// </ol>
    /// 
    /// <h3>More Advanced Usage</h3>
    /// 
    /// You may want to use <see cref="SetFieldNames"/> so you can examine
    /// multiple fields (e.g. body and title) for similarity.
    /// Depending on the size of your index and the size and makeup of your documents you
    /// may want to call the other set methods to control how the similarity queries are
    /// generated:
    /// <ul>
    /// <li> <see cref="MinTermFreq"/> </li>
    /// <li> <see cref="MinDocFreq"/> </li>
    /// <li> <see cref="MaxDocFreq"/></li>
    /// <li> <see cref="SetMaxDocFreqPct(int)"/></li>
    /// <li> <see cref="MinWordLen"/> </li>
    /// <li> <see cref="MaxWordLen"/></li>
    /// <li> <see cref="MaxQueryTerms"/></li>
    /// <li> <see cref="MaxNumTokensParsed"/></li>
    /// <li> <see cref="SetStopWords(ISet{string})"/> </li>
    /// </ul> 
    /// 
    /// <hr/>
    /// <pre>
    /// Changes: Mark Harwood 29/02/04
    /// Some bugfixing, some refactoring, some optimization.
    /// - bugfix: retrieveTerms(int docNum) was not working for indexes without a termvector -added missing code
    /// - bugfix: No significant terms being created for fields with a termvector - because 
    /// was only counting one occurrence per term/field pair in calculations(ie not including frequency info from TermVector) 
    /// - refactor: moved common code into isNoiseWord()
    /// - optimize: when no termvector support available - used maxNumTermsParsed to limit amount of tokenization
    /// </pre>
    /// </summary>
    public class LuceneMoreLikeThis : MoreLikeThisBase
    {
        /// <summary> For idf() calculations.</summary>
        private Similarity _similarity;

        private readonly IState _state;

        /// <summary> IndexReader to use</summary>
        private readonly IndexReader _ir;

        /// <summary> Analyzer that will be used to parse the doc.</summary>
        private Analyzer _analyzer = DEFAULT_ANALYZER;

        /// <summary> Constructor requiring an IndexReader.</summary>
        public LuceneMoreLikeThis(IndexReader ir, IState state)
            : this(ir, new DefaultSimilarity(), state)
        {
        }

        public LuceneMoreLikeThis(IndexReader ir, Similarity sim, IState state)
        {
            _ir = ir;
            _similarity = sim;
            _state = state;
        }

        public Similarity Similarity
        {
            get => _similarity;
            set => _similarity = value;
        }

        /// <summary> Gets or sets the analyzer used to parse source doc with. The default analyzer
        /// is the <see cref="DEFAULT_ANALYZER"/>.
        /// <para />
        /// An analyzer is not required for generating a query with the
        /// <see cref="Like(int)"/> method, all other 'like' methods require an analyzer.
        /// </summary>
        /// <value> the analyzer that will be used to parse source doc with. </value>
        /// <seealso cref="DEFAULT_ANALYZER">
        /// </seealso>
        public Analyzer Analyzer
        {
            get => _analyzer;
            set => _analyzer = value;
        }

        /// <summary>
        /// Set the maximum percentage in which words may still appear. Words that appear
        /// in more than this many percent of all docs will be ignored.
        /// </summary>
        /// <param name="maxPercentage">
        /// the maximum percentage of documents (0-100) that a term may appear 
        /// in to be still considered relevant
        /// </param>
        public override void SetMaxDocFreqPct(int maxPercentage)
        {
            _maxDocfreq = maxPercentage * _ir.NumDocs() / 100;
        }

        /// <summary>Return a query that will return docs like the passed lucene document ID.</summary>
        /// <param name="docNum">the documentID of the lucene doc to generate the 'More Like This" query for.</param>
        /// <returns> a query that will return docs like the passed lucene document ID.</returns>
        public Query Like(int docNum)
        {
            if (_fieldNames == null)
            {
                // gather list of valid fields from lucene
                var fields = _ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
                _fieldNames = fields.ToArray();
            }

            return CreateQuery(RetrieveTerms(docNum));
        }

        /// <summary> Return a query that will return docs like the passed file.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed file.
        /// </returns>
        public Query Like(FileInfo f)
        {
            if (_fieldNames == null)
            {
                // gather list of valid fields from lucene
                var fields = _ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
                _fieldNames = fields.ToArray();
            }

            using (var file = File.OpenRead(f.FullName))
            using (var reader = new StreamReader(file, Encoding.UTF8))
                return Like(reader);
        }

        /// <summary> Return a query that will return docs like the passed stream.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed stream.
        /// </returns>
        public Query Like(Stream isRenamed)
        {
            return Like(new StreamReader(isRenamed, Encoding.UTF8));
        }

        /// <summary> Return a query that will return docs like the passed Reader.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed Reader.
        /// </returns>
        public Query Like(TextReader r)
        {
            return CreateQuery(RetrieveTerms(r));
        }

        internal Query Like(BlittableJsonReaderObject json)
        {
            return CreateQuery(RetrieveTerms(json));
        }

        /// <summary> Create the More like query from a PriorityQueue</summary>
        private Query CreateQuery(PriorityQueue<object[]> q)
        {
            var query = new BooleanQuery();
            object cur;
            var qterms = 0;
            float bestScore = 0;

            while ((cur = q.Pop()) != null)
            {
                var ar = (object[])cur;
                var tq = new TermQuery(new Term((string)ar[1], (string)ar[0]));

                if (_boost)
                {
                    if (qterms == 0)
                    {
                        bestScore = (float)ar[2];
                    }
                    var myScore = (float)ar[2];

                    tq.Boost = _boostFactor * myScore / bestScore;
                }

                try
                {
                    query.Add(tq, Occur.SHOULD);
                }
                catch (BooleanQuery.TooManyClauses)
                {
                    break;
                }

                qterms++;
                if (_maxQueryTerms > 0 && qterms >= _maxQueryTerms)
                {
                    break;
                }
            }

            return query;
        }

        /// <summary> Create a PriorityQueue from a word->tf map.
        /// 
        /// </summary>
        /// <param name="words">a map of words keyed on the word(String) with Int objects as the values.
        /// </param>
        protected override PriorityQueue<object[]> CreateQueue(Dictionary<string, int> words)
        {
            // have collected all words in doc and their freqs
            var numDocs = _ir.NumDocs();
            var res = new FreqQ(words.Count); // will order words by score

            foreach(var (word, tf) in words)
            {
                // for every word
                if (_minTermFreq > 0 && tf < _minTermFreq)
                {
                    continue; // filter out words that don't occur enough times in the source
                }

                // go through all the fields and find the largest document frequency
                var topField = _fieldNames[0];
                var docFreq = 0;
                for (var i = 0; i < _fieldNames.Length; i++)
                {
                    var freq = _ir.DocFreq(new Term(_fieldNames[i], word), _state);
                    topField = freq > docFreq ? _fieldNames[i] : topField;
                    docFreq = freq > docFreq ? freq : docFreq;
                }

                if (_minDocFreq > 0 && docFreq < _minDocFreq)
                {
                    continue; // filter out words that don't occur in enough docs
                }

                if (docFreq > _maxDocfreq)
                {
                    continue; // filter out words that occur in too many docs
                }

                if (docFreq == 0)
                {
                    continue; // index update problem?
                }

                var idf = _similarity.Idf(docFreq, numDocs);
                var score = tf * idf;

                // only really need 1st 3 entries, other ones are for troubleshooting
                res.InsertWithOverflow(new object[] { word, topField, score, idf, docFreq, tf });
            }
            return res;
        }
        
        /// <summary> Find words for a more-like-this query former.
        /// 
        /// </summary>
        /// <param name="docNum">the id of the lucene document from which to find terms
        /// </param>
        protected virtual PriorityQueue<object[]> RetrieveTerms(int docNum)
        {
            Dictionary<string, int> termFreqMap = new();
            for (var i = 0; i < _fieldNames.Length; i++)
            {
                var fieldName = _fieldNames[i];
                var vector = _ir.GetTermFreqVector(docNum, fieldName, _state);

                // field does not store term vector info
                if (vector == null)
                {
                    var d = _ir.Document(docNum, _state);
                    var text = d.GetValues(fieldName, _state);
                    if (text != null)
                    {
                        for (var j = 0; j < text.Length; j++)
                        {
                            AddTermFrequencies(new StringReader(text[j]), termFreqMap, fieldName);
                        }
                    }
                }
                else
                {
                    AddTermFrequencies(termFreqMap, vector);
                }
            }

            return CreateQueue(termFreqMap);
        }

        /// <summary> Adds terms and frequencies found in vector into the Map termFreqMap</summary>
        /// <param name="termFreqMap">a Map of terms and their frequencies
        /// </param>
        /// <param name="vector">List of terms and their frequencies for a doc/field
        /// </param>
        protected void AddTermFrequencies(Dictionary<string, int> termFreqMap, ITermFreqVector vector)
        {
            var terms = vector.GetTerms();
            var freqs = vector.GetTermFrequencies();
            for (var j = 0; j < terms.Length; j++)
            {
                var term = terms[j];

                if (IsNoiseWord(term))
                {
                    continue;
                }
                // increment frequency
                ref var cnt = ref CollectionsMarshal.GetValueRefOrAddDefault(termFreqMap, term, out _);
                cnt += freqs[j];
            }
        }
        
        /// <summary> Adds term frequencies found by tokenizing text from reader into the Map words</summary>
        /// <param name="r">a source of text to be tokenized
        /// </param>
        /// <param name="termFreqMap">a Map of terms and their frequencies
        /// </param>
        /// <param name="fieldName">Used by analyzer for any special per-field analysis
        /// </param>
        protected override void AddTermFrequencies(TextReader r, Dictionary<string, int> termFreqMap, string fieldName)
        {
            var ts = _analyzer.TokenStream(fieldName, r);
            var tokenCount = 0;
            // for every token
            var termAtt = ts.AddAttribute<ITermAttribute>();

            while (ts.IncrementToken())
            {
                var word = termAtt.Term;
                tokenCount++;
                if (tokenCount > _maxNumTokensParsed)
                {
                    break;
                }
                if (IsNoiseWord(word))
                {
                    continue;
                }

                ref var cnt = ref CollectionsMarshal.GetValueRefOrAddDefault(termFreqMap, word, out _);
                cnt++;
            }
        }
        
        /// <summary> Find words for a more-like-this query former.
        /// The result is a priority queue of arrays with one entry for <b>every word</b> in the document.
        /// Each array has 6 elements.
        /// The elements are:
        /// <ol>
        /// <li> The word (String)</li>
        /// <li> The top field that this word comes from (String)</li>
        /// <li> The score for this word (Float)</li>
        /// <li> The IDF value (Float)</li>
        /// <li> The frequency of this word in the index (Integer)</li>
        /// <li> The frequency of this word in the source document (Integer)</li>
        /// </ol>
        /// This is a somewhat "advanced" routine, and in general only the 1st entry in the array is of interest.
        /// This method is exposed so that you can identify the "interesting words" in a document.
        /// For an easier method to call see <see cref="RetrieveInterestingTerms(System.IO.TextReader)"/>.
        /// 
        /// </summary>
        /// <param name="r">the reader that has the content of the document
        /// </param>
        /// <returns> the most intresting words in the document ordered by score, with the highest scoring, or best entry, first
        /// 
        /// </returns>
        /// <seealso cref="RetrieveInterestingTerms(System.IO.TextReader)">
        /// </seealso>
        public PriorityQueue<object[]> RetrieveTerms(TextReader r)
        {
            Dictionary<string, int> words = new();
            for (var i = 0; i < _fieldNames.Length; i++)
            {
                var fieldName = _fieldNames[i];
                AddTermFrequencies(r, words, fieldName);
            }
            return CreateQueue(words);
        }

        public string[] RetrieveInterestingTerms(int docNum)
        {
            var al = new List<object>(_maxQueryTerms);
            var pq = RetrieveTerms(docNum);
            object cur;
            var lim = _maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
            // we just want to return the top words
            while ((cur = pq.Pop()) != null && lim-- > 0)
            {
                var ar = (object[])cur;
                al.Add(ar[0]); // the 1st entry is the interesting word
            }
            //System.String[] res = new System.String[al.Count];
            //return al.toArray(res);
            return al.Select(x => x.ToString()).ToArray();
        }

        /// <summary> Convenience routine to make it easy to return the most interesting words in a document.
        /// More advanced users will call <see cref="RetrieveTerms(System.IO.TextReader)"/> directly.
        /// </summary>
        /// <param name="r">the source document
        /// </param>
        /// <returns> the most interesting words in the document
        /// 
        /// </returns>
        /// <seealso cref="RetrieveTerms(System.IO.TextReader)">
        /// </seealso>
        /// <seealso cref="MaxQueryTerms">
        /// </seealso>
        public string[] RetrieveInterestingTerms(TextReader r)
        {
            var al = new List<object>(_maxQueryTerms);
            var pq = RetrieveTerms(r);
            object cur;
            var lim = _maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
            // we just want to return the top words
            while ((cur = pq.Pop()) != null && lim-- > 0)
            {
                var ar = (object[])cur;
                al.Add(ar[0]); // the 1st entry is the interesting word
            }
            //System.String[] res = new System.String[al.Count];
            // return (System.String[]) SupportClass.ICollectionSupport.ToArray(al, res);
            return al.Select(x => x.ToString()).ToArray();
        }
    }
}
