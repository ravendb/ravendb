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
using Lucene.Net.Search;
using Lucene.Net.Util;
using Sparrow.Json;
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
    public abstract class MoreLikeThisBase
    {
        /// <summary> Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.</summary>
        /// <seealso cref="MaxNumTokensParsed">
        /// </seealso>
        public const int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;


        /// <summary> Default analyzer to parse source doc with.</summary>
        /// <seealso cref="Analyzer">
        /// </seealso>
        public static readonly Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_29);

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


        /// <summary> Current set of stop words.</summary>
        protected HashSet<string> _stopWords = null;

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



        /// <summary> Ignore words less frequent that this.</summary>
        protected int _minTermFreq = 0;

        /// <summary> Ignore words which do not occur in at least this many docs.</summary>
        protected int _minDocFreq = 0;

        /// <summary>
        /// Ignore words which occur in more than this many docs.
        /// </summary>
        protected int _maxDocfreq = DEFAULT_MAX_DOC_FREQ;

        /// <summary> Should we apply a boost to the Query based on the scores?</summary>
        protected bool _boost = DEFAULT_BOOST;

        /// <summary> Field name we'll analyze.</summary>
        protected string[] _fieldNames = null;

        /// <summary> The maximum number of tokens to parse in each example doc field that is not stored with TermVector support</summary>
        protected int _maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;



        /// <summary> Ignore words if less than this len.</summary>
        protected int _minWordLen = DEFAULT_MIN_WORD_LENGTH;

        /// <summary> Ignore words if greater than this len.</summary>
        protected int _maxWordLen = DEFAULT_MAX_WORD_LENGTH;

        /// <summary> Don't return a query longer than this.</summary>
        protected int _maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

        /// <summary> Boost factor to use when boosting the terms </summary>
        protected float _boostFactor = 1;

        /// <summary>
        /// Gets or sets the boost factor used when boosting terms
        /// </summary>
        public float BoostFactor
        {
            get => _boostFactor;
            set => _boostFactor = value;
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
        public void SetStopWords(HashSet<string> stopWords)
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

        /// <summary> Create a PriorityQueue from a word->tf map.
        /// 
        /// </summary>
        /// <param name="words">a map of words keyed on the word(String) with Int objects as the values.
        /// </param>
        protected abstract PriorityQueue<object[]> CreateQueue(Dictionary<string, int> words);

        protected static bool HasFlagWithBitPacking(BlittableJsonToken token)
        {
            return token.HasFlag(BlittableJsonToken.StartObject) &&
                   !token.HasFlag(BlittableJsonToken.String) &&
                   !token.HasFlag(BlittableJsonToken.Boolean) &&
                   !token.HasFlag(BlittableJsonToken.EmbeddedBlittable) &&
                   !token.HasFlag(BlittableJsonToken.Reserved2) &&
                   !token.HasFlag(BlittableJsonToken.Reserved4) &&
                   !token.HasFlag(BlittableJsonToken.Reserved6);
        }

        internal PriorityQueue<object[]> RetrieveTerms(BlittableJsonReaderObject json)
        {
            Dictionary<string, int> words = new();
            RetrieveTerms(json, words);

            return CreateQueue(words);
        }

        protected void RetrieveTerms(BlittableJsonReaderObject json, Dictionary<string, int> words)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref prop);

                ProcessTerms(prop.Token, prop.Name, prop.Value, words);
            }
        }

        public abstract void SetMaxDocFreqPct(int maxPercentage);

        protected abstract void AddTermFrequencies(TextReader r, Dictionary<string, int> termFreqMap, string fieldName);


        protected void ProcessTerms(BlittableJsonToken token, LazyStringValue name, object value, Dictionary<string, int> words)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.String:
                    var str = (LazyStringValue)value;
                    AddTermFrequencies(new StringReader(str), words, name);
                    return;
                case BlittableJsonToken.CompressedString:
                    var cstr = (LazyCompressedStringValue)value;
                    AddTermFrequencies(new StringReader(cstr), words, name);
                    return;
                case BlittableJsonToken.Integer:
                    AddTermFrequencies(new StringReader(((long)value).ToString()), words, name);
                    return;
                case BlittableJsonToken.LazyNumber:
                    AddTermFrequencies(new StringReader(((LazyNumberValue)value).ToString(CultureInfo.InvariantCulture)), words, name);
                    return;
                case BlittableJsonToken.Boolean:
                    AddTermFrequencies(new StringReader(((bool)value).ToString()), words, name);
                    return;
                case BlittableJsonToken.StartArray:
                    var array = (BlittableJsonReaderArray)value;
                    for (var j = 0; j < array.Length; j++)
                    {
                        var tuple = array.GetValueTokenTupleByIndex(j);
                        ProcessTerms(tuple.Item2, name, tuple.Item1, words);
                    }
                    return;
                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    RetrieveTerms((BlittableJsonReaderObject)value, words);
                    return;
            }

            if (HasFlagWithBitPacking(BlittableJsonToken.StartObject) ||
                HasFlagWithBitPacking(BlittableJsonToken.EmbeddedBlittable))
            {
                RetrieveTerms((BlittableJsonReaderObject)value, words);
            }
        }

        /// <summary> PriorityQueue that orders words by score.</summary>
        protected sealed class FreqQ : PriorityQueue<object[]>
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
    }
}
