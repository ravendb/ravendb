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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using TermQuery = Lucene.Net.Search.TermQuery;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Search.Similar
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
    /// 
    /// <p/>
    /// <h3>Initial Usage</h3>
    /// 
    /// This class has lots of options to try to make it efficient and flexible.
    /// See the body of <see cref="Main"/> below in the source for real code, or
    /// if you want pseudo code, the simpliest possible usage is as follows. The bold
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
    /// <li> get the text of the doc you want to find similaries to</li>
    /// <li> then call one of the Like() calls to generate a similarity query</li>
    /// <li> call the searcher to find the similar docs</li>
    /// </ol>
    /// 
    /// <h3>More Advanced Usage</h3>
    /// 
    /// You may want to use <see cref="SetFieldNames"/> so you can examine
    /// multiple fields (e.g. body and title) for similarity.
    /// <p/>
    /// 
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
    /// Some bugfixing, some refactoring, some optimisation.
    /// - bugfix: retrieveTerms(int docNum) was not working for indexes without a termvector -added missing code
    /// - bugfix: No significant terms being created for fields with a termvector - because 
    /// was only counting one occurence per term/field pair in calculations(ie not including frequency info from TermVector) 
    /// - refactor: moved common code into isNoiseWord()
    /// - optimise: when no termvector support available - used maxNumTermsParsed to limit amount of tokenization
    /// </pre>
    /// </summary>
    public sealed class MoreLikeThis
    {

        /// <summary> Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.</summary>
        /// <seealso cref="MaxNumTokensParsed">
        /// </seealso>
        public const int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;


        /// <summary> Default analyzer to parse source doc with.</summary>
        /// <seealso cref="Analyzer">
        /// </seealso>
        public static readonly Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Util.Version.LUCENE_CURRENT);

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
        public static readonly System.String[] DEFAULT_FIELD_NAMES = new System.String[] { "contents" };

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
        private ISet<string> stopWords = DEFAULT_STOP_WORDS;

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
        private Analyzer analyzer = DEFAULT_ANALYZER;

        /// <summary> Ignore words less freqent that this.</summary>
        private int minTermFreq = DEFAULT_MIN_TERM_FREQ;

        /// <summary> Ignore words which do not occur in at least this many docs.</summary>
        private int minDocFreq = DEFAULT_MIN_DOC_FREQ;

        /// <summary>
        /// Ignore words which occur in more than this many docs.
        /// </summary>
        private int maxDocfreq = DEFAULT_MAX_DOC_FREQ;

        /// <summary> Should we apply a boost to the Query based on the scores?</summary>
        private bool boost = DEFAULT_BOOST;

        /// <summary> Field name we'll analyze.</summary>
        private System.String[] fieldNames = DEFAULT_FIELD_NAMES;

        /// <summary> The maximum number of tokens to parse in each example doc field that is not stored with TermVector support</summary>
        private int maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;

        /// <summary> Ignore words if less than this len.</summary>
        private int minWordLen = DEFAULT_MIN_WORD_LENGTH;

        /// <summary> Ignore words if greater than this len.</summary>
        private int maxWordLen = DEFAULT_MAX_WORD_LENGTH;

        /// <summary> Don't return a query longer than this.</summary>
        private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

        /// <summary> For idf() calculations.</summary>
        private Lucene.Net.Search.Similarity similarity = null;

        /// <summary> IndexReader to use</summary>
        private IndexReader ir;

        /// <summary> Boost factor to use when boosting the terms </summary>
        private float boostFactor = 1;

        /// <summary>
        /// Gets or sets the boost factor used when boosting terms
        /// </summary>
        public float BoostFactor
        {
            get { return boostFactor; }
            set { this.boostFactor = value; }
        }

        /// <summary> Constructor requiring an IndexReader.</summary>
        public MoreLikeThis(IndexReader ir) : this(ir,new DefaultSimilarity())
        {
        }

        public MoreLikeThis(IndexReader ir, Lucene.Net.Search.Similarity sim)
        {
            this.ir = ir;
            this.similarity = sim;
        }

        public Similarity Similarity
        {
            get { return similarity; }
            set { this.similarity = value; }
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
            get { return analyzer; }
            set { this.analyzer = value; }
        }

        /// <summary>
        /// Gets or sets the frequency below which terms will be ignored in the source doc. The default
        /// frequency is the <see cref="DEFAULT_MIN_TERM_FREQ"/>.
        /// </summary>
        public int MinTermFreq
        {
            get { return minTermFreq; }
            set { this.minTermFreq = value; }
        }

        /// <summary>
        /// Gets or sets the frequency at which words will be ignored which do not occur in at least this
        /// many docs. The default frequency is <see cref="DEFAULT_MIN_DOC_FREQ"/>.
        /// </summary>
        public int MinDocFreq
        {
            get { return minDocFreq; }
            set { this.minDocFreq = value; }
        }

        /// <summary>
        /// Gets or sets the maximum frequency in which words may still appear. 
        /// Words that appear in more than this many docs will be ignored. The default frequency is 
        /// <see cref="DEFAULT_MAX_DOC_FREQ"/>
        /// </summary>
        public int MaxDocFreq
        {
            get { return this.maxDocfreq; }
            set { this.maxDocfreq = value; }
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
            this.maxDocfreq = maxPercentage * ir.NumDocs() / 100;
        }

        /// <summary> Gets or sets a boolean indicating whether to boost terms in query based 
        /// on "score" or not. The default is <see cref="DEFAULT_BOOST"/>.
        /// </summary>
        public bool Boost
        {
            get { return boost; }
            set { this.boost = value; }
        }

        /// <summary> Returns the field names that will be used when generating the 'More Like This' query.
        /// The default field names that will be used is <see cref="DEFAULT_FIELD_NAMES"/>.
        /// 
        /// </summary>
        /// <returns> the field names that will be used when generating the 'More Like This' query.
        /// </returns>
        public System.String[] GetFieldNames()
        {
            return fieldNames;
        }

        /// <summary> Sets the field names that will be used when generating the 'More Like This' query.
        /// Set this to null for the field names to be determined at runtime from the IndexReader
        /// provided in the constructor.
        /// 
        /// </summary>
        /// <param name="fieldNames">the field names that will be used when generating the 'More Like This'
        /// query.
        /// </param>
        public void SetFieldNames(System.String[] fieldNames)
        {
            this.fieldNames = fieldNames;
        }

        /// <summary>
        /// Gets or sets the minimum word length below which words will be ignored. 
        /// Set this to 0 for no minimum word length. The default is <see cref="DEFAULT_MIN_WORD_LENGTH"/>.
        /// </summary>
        public int MinWordLen
        {
            get { return minWordLen; }
            set { this.minWordLen = value; }
        }

        /// <summary>
        /// Gets or sets the maximum word length above which words will be ignored. Set this to 0 for no
        /// maximum word length. The default is <see cref="DEFAULT_MAX_WORD_LENGTH"/>.
        /// </summary>
        public int MaxWordLen
        {
            get { return maxWordLen; }
            set { this.maxWordLen = value; }
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
            this.stopWords = stopWords;
        }

        /// <summary> Get the current stop words being used.</summary>
        /// <seealso cref="SetStopWords">
        /// </seealso>
        public ISet<string> GetStopWords()
        {
            return stopWords;
        }


        /// <summary>
        /// Gets or sets the maximum number of query terms that will be included in any generated query.
        /// The default is <see cref="DEFAULT_MAX_QUERY_TERMS"/>.
        /// </summary>
        public int MaxQueryTerms
        {
            get { return maxQueryTerms; }
            set { this.maxQueryTerms = value; }
        }

        /// <summary>
        /// Gets or sets the maximum number of tokens to parse in each example doc
        /// field that is not stored with TermVector support
        /// </summary>
        /// <seealso cref="DEFAULT_MAX_NUM_TOKENS_PARSED" />
        public int MaxNumTokensParsed
        {
            get { return maxNumTokensParsed; }
            set { maxNumTokensParsed = value; }
        }

        /// <summary>Return a query that will return docs like the passed lucene document ID.</summary>
        /// <param name="docNum">the documentID of the lucene doc to generate the 'More Like This" query for.</param>
        /// <returns> a query that will return docs like the passed lucene document ID.</returns>
        public Query Like(int docNum)
        {
            if (fieldNames == null)
            {
                // gather list of valid fields from lucene
                ICollection<string> fields = ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
                fieldNames = fields.ToArray();
            }

            return CreateQuery(RetrieveTerms(docNum));
        }

        /// <summary> Return a query that will return docs like the passed file.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed file.
        /// </returns>
        public Query Like(System.IO.FileInfo f)
        {
            if (fieldNames == null)
            {
                // gather list of valid fields from lucene
                ICollection<string> fields = ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
                fieldNames = fields.ToArray();
            }

            return Like(new System.IO.StreamReader(f.FullName, System.Text.Encoding.Default));
        }

        /// <summary> Return a query that will return docs like the passed URL.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed URL.
        /// </returns>
        public Query Like(System.Uri u)
        {
            return Like(new System.IO.StreamReader((System.Net.WebRequest.Create(u)).GetResponse().GetResponseStream(), System.Text.Encoding.Default));
        }

        /// <summary> Return a query that will return docs like the passed stream.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed stream.
        /// </returns>
        public Query Like(System.IO.Stream is_Renamed)
        {
            return Like(new System.IO.StreamReader(is_Renamed, System.Text.Encoding.Default));
        }

        /// <summary> Return a query that will return docs like the passed Reader.
        /// 
        /// </summary>
        /// <returns> a query that will return docs like the passed Reader.
        /// </returns>
        public Query Like(System.IO.TextReader r)
        {
            return CreateQuery(RetrieveTerms(r));
        }

        /// <summary> Create the More like query from a PriorityQueue</summary>
        private Query CreateQuery(PriorityQueue<object[]> q)
        {
            BooleanQuery query = new BooleanQuery();
            System.Object cur;
            int qterms = 0;
            float bestScore = 0;

            while (((cur = q.Pop()) != null))
            {
                System.Object[] ar = (System.Object[])cur;
                TermQuery tq = new TermQuery(new Term((System.String)ar[1], (System.String)ar[0]));

                if (boost)
                {
                    if (qterms == 0)
                    {
                        bestScore = (float)ar[2];
                    }
                    float myScore = (float)ar[2];

                    tq.Boost = boostFactor * myScore / bestScore;
                }

                try
                {
                    query.Add(tq, Occur.SHOULD);
                }
                catch (BooleanQuery.TooManyClauses ignore)
                {
                    break;
                }

                qterms++;
                if (maxQueryTerms > 0 && qterms >= maxQueryTerms)
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
        private PriorityQueue<object[]> CreateQueue(IDictionary<string,Int> words)
        {
            // have collected all words in doc and their freqs
            int numDocs = ir.NumDocs();
            FreqQ res = new FreqQ(words.Count); // will order words by score

            var it = words.Keys.GetEnumerator();
            while (it.MoveNext())
            {
                // for every word
                System.String word = it.Current;

                int tf = words[word].x; // term freq in the source doc
                if (minTermFreq > 0 && tf < minTermFreq)
                {
                    continue; // filter out words that don't occur enough times in the source
                }

                // go through all the fields and find the largest document frequency
                System.String topField = fieldNames[0];
                int docFreq = 0;
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    int freq = ir.DocFreq(new Term(fieldNames[i], word));
                    topField = (freq > docFreq) ? fieldNames[i] : topField;
                    docFreq = (freq > docFreq) ? freq : docFreq;
                }

                if (minDocFreq > 0 && docFreq < minDocFreq)
                {
                    continue; // filter out words that don't occur in enough docs
                }

                if (docFreq > maxDocfreq)
                {
                    continue; // filter out words that occur in too many docs
                }

                if (docFreq == 0)
                {
                    continue; // index update problem?
                }

                float idf = similarity.Idf(docFreq, numDocs);
                float score = tf * idf;

                // only really need 1st 3 entries, other ones are for troubleshooting
                res.InsertWithOverflow(new System.Object[] { word, topField, score, idf, docFreq, tf });
            }
            return res;
        }

        /// <summary> Describe the parameters that control how the "more like this" query is formed.</summary>
        public System.String DescribeParams()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.Append("\t" + "maxQueryTerms  : " + maxQueryTerms + "\n");
            sb.Append("\t" + "minWordLen     : " + minWordLen + "\n");
            sb.Append("\t" + "maxWordLen     : " + maxWordLen + "\n");
            sb.Append("\t" + "fieldNames     : \"");
            System.String delim = "";
            for (int i = 0; i < fieldNames.Length; i++)
            {
                System.String fieldName = fieldNames[i];
                sb.Append(delim).Append(fieldName);
                delim = ", ";
            }
            sb.Append("\n");
            sb.Append("\t" + "boost          : " + boost + "\n");
            sb.Append("\t" + "minTermFreq    : " + minTermFreq + "\n");
            sb.Append("\t" + "minDocFreq     : " + minDocFreq + "\n");
            return sb.ToString();
        }

        /// <summary> Test driver.
        /// Pass in "-i INDEX" and then either "-fn FILE" or "-url URL".
        /// </summary>
        [STAThread]
        public static void Main(System.String[] a)
        {
            System.String indexName = "localhost_index";
            System.String fn = "c:/Program Files/Apache Group/Apache/htdocs/manual/vhosts/index.html.en";
            System.Uri url = null;
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i].Equals("-i"))
                {
                    indexName = a[++i];
                }
                else if (a[i].Equals("-f"))
                {
                    fn = a[++i];
                }
                else if (a[i].Equals("-url"))
                {
                    url = new System.Uri(a[++i]);
                }
            }

            System.IO.StreamWriter temp_writer;
            temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardOutput(), System.Console.Out.Encoding);
            temp_writer.AutoFlush = true;
            System.IO.StreamWriter o = temp_writer;
            FSDirectory dir = FSDirectory.Open(new DirectoryInfo(indexName));
            IndexReader r = IndexReader.Open(dir, true);
            o.WriteLine("Open index " + indexName + " which has " + r.NumDocs() + " docs");

            MoreLikeThis mlt = new MoreLikeThis(r);

            o.WriteLine("Query generation parameters:");
            o.WriteLine(mlt.DescribeParams());
            o.WriteLine();

            Query query = null;
            if (url != null)
            {
                o.WriteLine("Parsing URL: " + url);
                query = mlt.Like(url);
            }
            else if (fn != null)
            {
                o.WriteLine("Parsing file: " + fn);
                query = mlt.Like(new System.IO.FileInfo(fn));
            }

            o.WriteLine("q: " + query);
            o.WriteLine();
            IndexSearcher searcher = new IndexSearcher(dir, true);

            TopDocs hits = searcher.Search(query, null, 25);
            int len = hits.TotalHits;
            o.WriteLine("found: " + len + " documents matching");
            o.WriteLine();
            ScoreDoc[] scoreDocs = hits.ScoreDocs;
            for (int i = 0; i < System.Math.Min(25, len); i++)
            {
                Document d = searcher.Doc(scoreDocs[i].Doc);
                System.String summary = d.Get("summary");
                o.WriteLine("score  : " + scoreDocs[i].Score);
                o.WriteLine("url    : " + d.Get("url"));
                o.WriteLine("\ttitle  : " + d.Get("title"));
                if (summary != null)
                    o.WriteLine("\tsummary: " + d.Get("summary"));
                o.WriteLine();
            }
        }

        /// <summary> Find words for a more-like-this query former.
        /// 
        /// </summary>
        /// <param name="docNum">the id of the lucene document from which to find terms
        /// </param>
        private PriorityQueue<object[]> RetrieveTerms(int docNum)
        {
            IDictionary<string,Int> termFreqMap = new HashMap<string,Int>();
            for (int i = 0; i < fieldNames.Length; i++)
            {
                System.String fieldName = fieldNames[i];
                ITermFreqVector vector = ir.GetTermFreqVector(docNum, fieldName);

                // field does not store term vector info
                if (vector == null)
                {
                    Document d = ir.Document(docNum);
                    System.String[] text = d.GetValues(fieldName);
                    if (text != null)
                    {
                        for (int j = 0; j < text.Length; j++)
                        {
                            AddTermFrequencies(new System.IO.StringReader(text[j]), termFreqMap, fieldName);
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
        private void AddTermFrequencies(IDictionary<string, Int> termFreqMap, ITermFreqVector vector)
        {
            System.String[] terms = vector.GetTerms();
            int[] freqs = vector.GetTermFrequencies();
            for (int j = 0; j < terms.Length; j++)
            {
                System.String term = terms[j];

                if (IsNoiseWord(term))
                {
                    continue;
                }
                // increment frequency
                Int cnt = termFreqMap[term];
                if (cnt == null)
                {
                    cnt = new Int();
                    termFreqMap[term] = cnt;
                    cnt.x = freqs[j];
                }
                else
                {
                    cnt.x += freqs[j];
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
        private void AddTermFrequencies(System.IO.TextReader r, IDictionary<string,Int> termFreqMap, System.String fieldName)
        {
            TokenStream ts = analyzer.TokenStream(fieldName, r);
			int tokenCount=0;
			// for every token
            ITermAttribute termAtt = ts.AddAttribute<ITermAttribute>();
			
			while (ts.IncrementToken()) {
				string word = termAtt.Term;
				tokenCount++;
				if(tokenCount>maxNumTokensParsed)
				{
					break;
				}
				if(IsNoiseWord(word)){
					continue;
				}
				
				// increment frequency
				Int cnt = termFreqMap[word];
				if (cnt == null) {
                    termFreqMap[word] = new Int();
				}
				else {
					cnt.x++;
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
        private bool IsNoiseWord(System.String term)
        {
            int len = term.Length;
            if (minWordLen > 0 && len < minWordLen)
            {
                return true;
            }
            if (maxWordLen > 0 && len > maxWordLen)
            {
                return true;
            }
            if (stopWords != null && stopWords.Contains(term))
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
        public PriorityQueue<object[]> RetrieveTerms(System.IO.TextReader r)
        {
            IDictionary<string, Int> words = new HashMap<string,Int>();
            for (int i = 0; i < fieldNames.Length; i++)
            {
                System.String fieldName = fieldNames[i];
                AddTermFrequencies(r, words, fieldName);
            }
            return CreateQueue(words);
        }


        public System.String[] RetrieveInterestingTerms(int docNum)
        {
            List<object> al = new List<object>(maxQueryTerms);
            PriorityQueue<object[]> pq = RetrieveTerms(docNum);
            System.Object cur;
            int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
            // we just want to return the top words
            while (((cur = pq.Pop()) != null) && lim-- > 0)
            {
                System.Object[] ar = (System.Object[])cur;
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
        public System.String[] RetrieveInterestingTerms(System.IO.TextReader r)
        {
            List<object> al = new List<object>(maxQueryTerms);
            PriorityQueue<object[]> pq = RetrieveTerms(r);
            System.Object cur;
            int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
            // we just want to return the top words
            while (((cur = pq.Pop()) != null) && lim-- > 0)
            {
                System.Object[] ar = (System.Object[])cur;
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

            override public bool LessThan(System.Object[] aa, System.Object[] bb)
            {
                float fa = (float)aa[2];
                float fb = (float)bb[2];
                return (float)fa > (float)fb;
            }
        }

        /// <summary> Use for frequencies and to avoid renewing Integers.</summary>
        private class Int
        {
            internal int x;

            internal Int()
            {
                x = 1;
            }
        }
    }
}