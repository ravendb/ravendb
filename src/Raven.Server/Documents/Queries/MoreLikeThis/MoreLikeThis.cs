﻿/*
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
    public class MoreLikeThis
    {

        /// <summary> Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.</summary>
        /// <seealso cref="MaxNumTokensParsed">
        /// </seealso>
        public const int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;


        /// <summary> Default analyzer to parse source doc with.</summary>
        /// <seealso cref="Analyzer">
        /// </seealso>
        public static readonly Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_29);

        /// <summary> Ignore terms with less than this frequency in the source doc.</summary>
        /// <seealso cref="MinTermFreq">
        /// </seealso>
        /// <seealso cref="MinTermFreq">
        /// </seealso>
        public const int DEFAULT_MIN_TERM_FREQ = 2;

        /// <summary> Ignore words which do not occur in at least this many docs.</summary>
        /// <seealso cref="MinDocFreq">
        /// </seealso>
        /// <seealso cref="MinDocFreq">
        /// </seealso>
        public const int DEFAULT_MIN_DOC_FREQ = 5;

        /// <summary>
        /// Ignore words wich occur in more than this many docs
        /// </summary>
        /// <seealso cref="MaxDocFreq"/>
        /// <seealso cref="MaxDocFreq"/>
        public const int DEFAULT_MAX_DOC_FREQ = int.MaxValue;

        /// <summary> Boost terms in query based on score.</summary>
        /// <seealso cref="Boost">
        /// </seealso>
        /// <seealso cref="Boost">
        /// </seealso>
        public const bool DEFAULT_BOOST = false;

        /// <summary> Default field names. Null is used to specify that the field names should be looked
        /// up at runtime from the provided reader.
        /// </summary>
        public static readonly string[] DEFAULT_FIELD_NAMES = { "contents" };

        /// <summary> Ignore words less than this length or if 0 then this has no effect.</summary>
        /// <seealso cref="MinWordLen">
        /// </seealso>
        /// <seealso cref="MinWordLen">
        /// </seealso>
        public const int DEFAULT_MIN_WORD_LENGTH = 0;

        /// <summary> Ignore words greater than this length or if 0 then this has no effect.</summary>
        /// <seealso cref="MaxWordLen">
        /// </seealso>
        /// <seealso cref="MaxWordLen">
        /// </seealso>
        public const int DEFAULT_MAX_WORD_LENGTH = 0;

        /// <summary> Default set of stopwords.
        /// If null means to allow stop words.
        /// 
        /// </summary>
        /// <seealso cref="SetStopWords">
        /// </seealso>
        /// <seealso cref="GetStopWords">
        /// </seealso>
        public static readonly ISet<string> DEFAULT_STOP_WORDS = null;

        /// <summary> Current set of stop words.</summary>
        private ISet<string> _stopWords = DEFAULT_STOP_WORDS;

        /// <summary> Return a Query with no more than this many terms.
        /// 
        /// </summary>
        /// <seealso cref="BooleanQuery.MaxClauseCount">
        /// </seealso>
        /// <seealso cref="MaxQueryTerms">
        /// </seealso>
        /// <seealso cref="MaxQueryTerms">
        /// </seealso>
        public const int DEFAULT_MAX_QUERY_TERMS = 25;

        /// <summary> Analyzer that will be used to parse the doc.</summary>
        private Analyzer _analyzer = DEFAULT_ANALYZER;

        /// <summary> Ignore words less frequent that this.</summary>
        private int _minTermFreq = DEFAULT_MIN_TERM_FREQ;

        /// <summary> Ignore words which do not occur in at least this many docs.</summary>
        private int _minDocFreq = DEFAULT_MIN_DOC_FREQ;

        /// <summary>
        /// Ignore words which occur in more than this many docs.
        /// </summary>
        private int _maxDocfreq = DEFAULT_MAX_DOC_FREQ;

        /// <summary> Should we apply a boost to the Query based on the scores?</summary>
        private bool _boost = DEFAULT_BOOST;

        /// <summary> Field name we'll analyze.</summary>
        private string[] _fieldNames = DEFAULT_FIELD_NAMES;

        /// <summary> The maximum number of tokens to parse in each example doc field that is not stored with TermVector support</summary>
        private int _maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;



        /// <summary> Ignore words if less than this len.</summary>
        private int _minWordLen = DEFAULT_MIN_WORD_LENGTH;

        /// <summary> Ignore words if greater than this len.</summary>
        private int _maxWordLen = DEFAULT_MAX_WORD_LENGTH;

        /// <summary> Don't return a query longer than this.</summary>
        private int _maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

        /// <summary> For idf() calculations.</summary>
        private Similarity _similarity;

        private readonly IState _state;

        /// <summary> IndexReader to use</summary>
        private readonly IndexReader _ir;

        /// <summary> Boost factor to use when boosting the terms </summary>
        private float _boostFactor = 1;

        /// <summary>
        /// Gets or sets the boost factor used when boosting terms
        /// </summary>
        public float BoostFactor
        {
            get => _boostFactor;
            set => _boostFactor = value;
        }

        /// <summary> Constructor requiring an IndexReader.</summary>
        public MoreLikeThis(IndexReader ir, IState state)
            : this(ir, new DefaultSimilarity(), state)
        {
        }

        public MoreLikeThis(IndexReader ir, Similarity sim, IState state)
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
        /// Gets or sets the frequency below which terms will be ignored in the source doc. The default
        /// frequency is the <see cref="DEFAULT_MIN_TERM_FREQ"/>.
        /// </summary>
        public int MinTermFreq
        {
            get => _minTermFreq;
            set => _minTermFreq = value;
        }

        /// <summary>
        /// Gets or sets the frequency at which words will be ignored which do not occur in at least this
        /// many docs. The default frequency is <see cref="DEFAULT_MIN_DOC_FREQ"/>.
        /// </summary>
        public int MinDocFreq
        {
            get => _minDocFreq;
            set => _minDocFreq = value;
        }

        /// <summary>
        /// Gets or sets the maximum frequency in which words may still appear. 
        /// Words that appear in more than this many docs will be ignored. The default frequency is 
        /// <see cref="DEFAULT_MAX_DOC_FREQ"/>
        /// </summary>
        public int MaxDocFreq
        {
            get => _maxDocfreq;
            set => _maxDocfreq = value;
        }

        /// <summary>
        /// Set the maximum percentage in which words may still appear. Words that appear
        /// in more than this many percent of all docs will be ignored.
        /// </summary>
        /// <param name="maxPercentage">
        /// the maximum percentage of documents (0-100) that a term may appear 
        /// in to be still considered relevant
        /// </param>
        public void SetMaxDocFreqPct(int maxPercentage)
        {
            _maxDocfreq = maxPercentage * _ir.NumDocs() / 100;
        }

        /// <summary> Gets or sets a boolean indicating whether to boost terms in query based 
        /// on "score" or not. The default is <see cref="DEFAULT_BOOST"/>.
        /// </summary>
        public bool Boost
        {
            get => _boost;
            set => _boost = value;
        }

        /// <summary> Returns the field names that will be used when generating the 'More Like This' query.
        /// The default field names that will be used is <see cref="DEFAULT_FIELD_NAMES"/>.
        /// 
        /// </summary>
        /// <returns> the field names that will be used when generating the 'More Like This' query.
        /// </returns>
        public string[] GetFieldNames()
        {
            return _fieldNames;
        }

        /// <summary> Sets the field names that will be used when generating the 'More Like This' query.
        /// Set this to null for the field names to be determined at runtime from the IndexReader
        /// provided in the constructor.
        /// 
        /// </summary>
        /// <param name="fieldNames">the field names that will be used when generating the 'More Like This'
        /// query.
        /// </param>
        public void SetFieldNames(string[] fieldNames)
        {
            _fieldNames = fieldNames;
        }

        /// <summary>
        /// Gets or sets the minimum word length below which words will be ignored. 
        /// Set this to 0 for no minimum word length. The default is <see cref="DEFAULT_MIN_WORD_LENGTH"/>.
        /// </summary>
        public int MinWordLen
        {
            get => _minWordLen;
            set => _minWordLen = value;
        }

        /// <summary>
        /// Gets or sets the maximum word length above which words will be ignored. Set this to 0 for no
        /// maximum word length. The default is <see cref="DEFAULT_MAX_WORD_LENGTH"/>.
        /// </summary>
        public int MaxWordLen
        {
            get => _maxWordLen;
            set => _maxWordLen = value;
        }

        /// <summary> Set the set of stopwords.
        /// Any word in this set is considered "uninteresting" and ignored.
        /// Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
        /// for the purposes of document similarity it seems reasonable to assume that "a stop word is never interesting".
        /// 
        /// </summary>
        /// <param name="stopWords">set of stopwords, if null it means to allow stop words
        /// 
        /// </param>
        /// <seealso cref="Lucene.Net.Analysis.StopFilter.MakeStopSet(string[])">
        /// </seealso>
        /// <seealso cref="GetStopWords">
        /// </seealso>
        public void SetStopWords(ISet<string> stopWords)
        {
            _stopWords = stopWords;
        }

        /// <summary> Get the current stop words being used.</summary>
        /// <seealso cref="SetStopWords">
        /// </seealso>
        public ISet<string> GetStopWords()
        {
            return _stopWords;
        }


        /// <summary>
        /// Gets or sets the maximum number of query terms that will be included in any generated query.
        /// The default is <see cref="DEFAULT_MAX_QUERY_TERMS"/>.
        /// </summary>
        public int MaxQueryTerms
        {
            get => _maxQueryTerms;
            set => _maxQueryTerms = value;
        }

        /// <summary>
        /// Gets or sets the maximum number of tokens to parse in each example doc
        /// field that is not stored with TermVector support
        /// </summary>
        /// <seealso cref="DEFAULT_MAX_NUM_TOKENS_PARSED" />
        public int MaxNumTokensParsed
        {
            get => _maxNumTokensParsed;
            set => _maxNumTokensParsed = value;
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
        protected PriorityQueue<object[]> CreateQueue(IDictionary<string, Int> words)
        {
            // have collected all words in doc and their freqs
            var numDocs = _ir.NumDocs();
            var res = new FreqQ(words.Count); // will order words by score

            var it = words.Keys.GetEnumerator();
            while (it.MoveNext())
            {
                // for every word
                var word = it.Current;

                var tf = words[word].X; // term freq in the source doc
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

        /// <summary> Describe the parameters that control how the "more like this" query is formed.</summary>
        public string DescribeParams()
        {
            var sb = new StringBuilder();
            sb.Append("\t" + "maxQueryTerms  : " + _maxQueryTerms + "\n");
            sb.Append("\t" + "minWordLen     : " + _minWordLen + "\n");
            sb.Append("\t" + "maxWordLen     : " + _maxWordLen + "\n");
            sb.Append("\t" + "fieldNames     : \"");
            var delim = "";
            for (var i = 0; i < _fieldNames.Length; i++)
            {
                var fieldName = _fieldNames[i];
                sb.Append(delim).Append(fieldName);
                delim = ", ";
            }
            sb.Append("\n");
            sb.Append("\t" + "boost          : " + _boost + "\n");
            sb.Append("\t" + "minTermFreq    : " + _minTermFreq + "\n");
            sb.Append("\t" + "minDocFreq     : " + _minDocFreq + "\n");
            return sb.ToString();
        }

        /// <summary> Find words for a more-like-this query former.
        /// 
        /// </summary>
        /// <param name="docNum">the id of the lucene document from which to find terms
        /// </param>
        protected virtual PriorityQueue<object[]> RetrieveTerms(int docNum)
        {
            IDictionary<string, Int> termFreqMap = new HashMap<string, Int>();
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
        protected void AddTermFrequencies(IDictionary<string, Int> termFreqMap, ITermFreqVector vector)
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
                var cnt = termFreqMap[term];
                if (cnt == null)
                {
                    cnt = new Int();
                    termFreqMap[term] = cnt;
                    cnt.X = freqs[j];
                }
                else
                {
                    cnt.X += freqs[j];
                }
            }
        }
        /// <summary> Adds term frequencies found by tokenizing text from reader into the Map words</summary>
        /// <param name="r">a source of text to be tokenized
        /// </param>
        /// <param name="termFreqMap">a Map of terms and their frequencies
        /// </param>
        /// <param name="fieldName">Used by analyzer for any special per-field analysis
        /// </param>
        protected void AddTermFrequencies(TextReader r, IDictionary<string, Int> termFreqMap, string fieldName)
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

                // increment frequency
                var cnt = termFreqMap[word];
                if (cnt == null)
                {
                    termFreqMap[word] = new Int();
                }
                else
                {
                    cnt.X++;
                }
            }
        }


        /// <summary>determines if the passed term is likely to be of interest in "more like" comparisons 
        /// 
        /// </summary>
        /// <param name="term">The word being considered
        /// </param>
        /// <returns> true if should be ignored, false if should be used in further analysis
        /// </returns>
        protected bool IsNoiseWord(string term)
        {
            var len = term.Length;
            if (_minWordLen > 0 && len < _minWordLen)
            {
                return true;
            }
            if (_maxWordLen > 0 && len > _maxWordLen)
            {
                return true;
            }
            if (_stopWords != null && _stopWords.Contains(term))
            {
                return true;
            }
            return false;
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
            IDictionary<string, Int> words = new HashMap<string, Int>();
            for (var i = 0; i < _fieldNames.Length; i++)
            {
                var fieldName = _fieldNames[i];
                AddTermFrequencies(r, words, fieldName);
            }
            return CreateQueue(words);
        }


        internal PriorityQueue<object[]> RetrieveTerms(BlittableJsonReaderObject json)
        {
            IDictionary<string, Int> words = new HashMap<string, Int>();
            RetrieveTerms(json, words);

            return CreateQueue(words);
        }

        private void RetrieveTerms(BlittableJsonReaderObject json, IDictionary<string, Int> words)
        {
            for (int i = 0; i < json.Count; i++)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                json.GetPropertyByIndex(i, ref prop);

                switch (prop.Token)
                {
                    case BlittableJsonToken.String:
                        var str = (LazyStringValue)prop.Value;
                        AddTermFrequencies(new StringReader(str),words, prop.Name);
                        continue;
                    case BlittableJsonToken.CompressedString:
                        var cstr = (LazyCompressedStringValue)prop.Value;
                        AddTermFrequencies(new StringReader(cstr), words, prop.Name);
                        continue;
                    case BlittableJsonToken.Integer:
                        AddTermFrequencies(new StringReader(((long)prop.Value).ToString()), words, prop.Name);
                        continue;
                    case BlittableJsonToken.LazyNumber:
                        AddTermFrequencies(new StringReader(((LazyNumberValue)prop.Value).ToString(CultureInfo.InvariantCulture)), words, prop.Name);
                        continue;
                    case BlittableJsonToken.Boolean:
                        AddTermFrequencies(new StringReader(((bool)prop.Value).ToString()), words, prop.Name);
                        continue;
                    case BlittableJsonToken.StartArray:
                        foreach (BlittableJsonReaderObject item in (BlittableJsonReaderArray)prop.Value)
                        {
                            RetrieveTerms(item, words);
                        }
                        continue;
                    case BlittableJsonToken.EmbeddedBlittable:
                    case BlittableJsonToken.StartObject:
                        RetrieveTerms((BlittableJsonReaderObject)prop.Value, words);
                        continue;
                }

                if (HasFlagWithBitPacking(BlittableJsonToken.StartObject) ||
                    HasFlagWithBitPacking(BlittableJsonToken.EmbeddedBlittable))
                {
                    RetrieveTerms((BlittableJsonReaderObject)prop.Value, words);
                }
            }
        }

        private static bool HasFlagWithBitPacking(BlittableJsonToken token)
        {
            return token.HasFlag(BlittableJsonToken.StartObject) &&
                   !token.HasFlag(BlittableJsonToken.String) &&
                   !token.HasFlag(BlittableJsonToken.Boolean) &&
                   !token.HasFlag(BlittableJsonToken.EmbeddedBlittable) &&
                   !token.HasFlag(BlittableJsonToken.Reserved2) &&
                   !token.HasFlag(BlittableJsonToken.Reserved4) &&
                   !token.HasFlag(BlittableJsonToken.Reserved6);
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

        /// <summary> PriorityQueue that orders words by score.</summary>
        private class FreqQ : PriorityQueue<object[]>
        {
            internal FreqQ(int s)
            {
                Initialize(s);
            }

            public override bool LessThan(object[] aa, object[] bb)
            {
                var fa = (float)aa[2];
                var fb = (float)bb[2];
                return (float)fa > (float)fb;
            }
        }

        /// <summary> Use for frequencies and to avoid renewing Integers.</summary>
        protected class Int
        {
            internal int X;

            internal Int()
            {
                X = 1;
            }
        }
    }
}
