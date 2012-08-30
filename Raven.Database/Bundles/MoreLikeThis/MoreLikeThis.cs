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

using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using TermQuery = Lucene.Net.Search.TermQuery;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using Query = Lucene.Net.Search.Query;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Version = Lucene.Net.Util.Version;

namespace Similarity.Net
{


	/// <summary> Generate "more like this" similarity queries. 
	/// Based on this mail:
	/// <code><pre>
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
	/// </pre></code>
	/// 
	/// 
	/// <p>
	/// <h3>Initial Usage</h3>
	/// 
	/// This class has lots of options to try to make it efficient and flexible.
	/// See the body of {@link #main Main()} below in the source for real code, or
	/// if you want pseudo code, the simpliest possible usage is as follows. The bold
	/// fragment is specific to this class.
	/// 
	/// <code><pre>
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
	/// </pre></code>
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
	/// You may want to use {@link #SetFieldNames SetFieldNames(...)} so you can examine
	/// multiple fields (e.g. body and title) for similarity.
	/// <p>
	/// 
	/// Depending on the size of your index and the size and makeup of your documents you
	/// may want to call the other set methods to control how the similarity queries are
	/// generated:
	/// <ul>
	/// <li> {@link #SetMinTermFreq SetMinTermFreq(...)}</li>
	/// <li> {@link #SetMinDocFreq SetMinDocFreq(...)}</li>
	/// <li> {@link #SetMinWordLen SetMinWordLen(...)}</li>
	/// <li> {@link #SetMaxWordLen SetMaxWordLen(...)}</li>
	/// <li> {@link #SetMaxQueryTerms SetMaxQueryTerms(...)}</li>
	/// <li> {@link #SetMaxNumTokensParsed SetMaxNumTokensParsed(...)}</li>
	/// <li> {@link #SetStopWords SetStopWord(...)} </li>
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
	/// 
	/// </summary>
	/// <author>  David Spencer
	/// </author>
	/// <author>  Bruce Ritchie
	/// </author>
	/// <author>  Mark Harwood
	/// </author>
	public class MoreLikeThis
	{

		/// <summary> Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.</summary>
		/// <seealso cref="#getMaxNumTokensParsed">
		/// </seealso>
		public const int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;


		/// <summary> Default analyzer to parse source doc with.</summary>
		/// <seealso cref="#getAnalyzer">
		/// </seealso>
		public static readonly Analyzer DEFAULT_ANALYZER = new StandardAnalyzer(Version.LUCENE_29);

		/// <summary> Ignore terms with less than this frequency in the source doc.</summary>
		/// <seealso cref="#getMinTermFreq">
		/// </seealso>
		/// <seealso cref="#setMinTermFreq">
		/// </seealso>
		public const int DEFAULT_MIN_TERM_FREQ = 2;

		/// <summary> Ignore words which do not occur in at least this many docs.</summary>
		/// <seealso cref="#getMinDocFreq">
		/// </seealso>
		/// <seealso cref="#setMinDocFreq">
		/// </seealso>
		public const int DEFALT_MIN_DOC_FREQ = 5;

		/// <summary> Boost terms in query based on score.</summary>
		/// <seealso cref="#isBoost">
		/// </seealso>
		/// <seealso cref="#SetBoost">
		/// </seealso>
		public const bool DEFAULT_BOOST = false;

		/// <summary> Default field names. Null is used to specify that the field names should be looked
		/// up at runtime from the provided reader.
		/// </summary>
		public static readonly System.String[] DEFAULT_FIELD_NAMES = new System.String[] { "contents" };

		/// <summary> Ignore words less than this length or if 0 then this has no effect.</summary>
		/// <seealso cref="#getMinWordLen">
		/// </seealso>
		/// <seealso cref="#setMinWordLen">
		/// </seealso>
		public const int DEFAULT_MIN_WORD_LENGTH = 0;

		/// <summary> Ignore words greater than this length or if 0 then this has no effect.</summary>
		/// <seealso cref="#getMaxWordLen">
		/// </seealso>
		/// <seealso cref="#setMaxWordLen">
		/// </seealso>
		public const int DEFAULT_MAX_WORD_LENGTH = 0;

		/// <summary> Default set of stopwords.
		/// If null means to allow stop words.
		/// 
		/// </summary>
		/// <seealso cref="#setStopWords">
		/// </seealso>
		/// <seealso cref="#getStopWords">
		/// </seealso>
		public static readonly System.Collections.Hashtable DEFAULT_STOP_WORDS = null;

		/// <summary> Current set of stop words.</summary>
		private System.Collections.Hashtable stopWords = DEFAULT_STOP_WORDS;

		/// <summary> Return a Query with no more than this many terms.
		/// 
		/// </summary>
		/// <seealso cref="BooleanQuery#getMaxClauseCount">
		/// </seealso>
		/// <seealso cref="#getMaxQueryTerms">
		/// </seealso>
		/// <seealso cref="#setMaxQueryTerms">
		/// </seealso>
		public const int DEFAULT_MAX_QUERY_TERMS = 25;

		/// <summary> Analyzer that will be used to parse the doc.</summary>
		private Analyzer analyzer = DEFAULT_ANALYZER;

		/// <summary> Ignore words less freqent that this.</summary>
		private int minTermFreq = DEFAULT_MIN_TERM_FREQ;

		/// <summary> Ignore words which do not occur in at least this many docs.</summary>
		private int minDocFreq = DEFALT_MIN_DOC_FREQ;

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
		private Lucene.Net.Search.Similarity similarity = new DefaultSimilarity();

		/// <summary> IndexReader to use</summary>
		private IndexReader ir;

		/// <summary> Constructor requiring an IndexReader.</summary>
		public MoreLikeThis(IndexReader ir)
		{
			this.ir = ir;
		}

		/// <summary> Returns an analyzer that will be used to parse source doc with. The default analyzer
		/// is the {@link #DEFAULT_ANALYZER}.
		/// 
		/// </summary>
		/// <returns> the analyzer that will be used to parse source doc with.
		/// </returns>
		/// <seealso cref="#DEFAULT_ANALYZER">
		/// </seealso>
		public Analyzer GetAnalyzer()
		{
			return analyzer;
		}

		/// <summary> Sets the analyzer to use. An analyzer is not required for generating a query with the
		/// {@link #Like(int)} method, all other 'like' methods require an analyzer.
		/// 
		/// </summary>
		/// <param name="analyzer">the analyzer to use to tokenize text.
		/// </param>
		public void SetAnalyzer(Analyzer analyzer)
		{
			this.analyzer = analyzer;
		}

		/// <summary> Returns the frequency below which terms will be ignored in the source doc. The default
		/// frequency is the {@link #DEFAULT_MIN_TERM_FREQ}.
		/// 
		/// </summary>
		/// <returns> the frequency below which terms will be ignored in the source doc.
		/// </returns>
		public int GetMinTermFreq()
		{
			return minTermFreq;
		}

		/// <summary> Sets the frequency below which terms will be ignored in the source doc.
		/// 
		/// </summary>
		/// <param name="minTermFreq">the frequency below which terms will be ignored in the source doc.
		/// </param>
		public void SetMinTermFreq(int minTermFreq)
		{
			this.minTermFreq = minTermFreq;
		}

		/// <summary> Returns the frequency at which words will be ignored which do not occur in at least this
		/// many docs. The default frequency is {@link #DEFALT_MIN_DOC_FREQ}.
		/// 
		/// </summary>
		/// <returns> the frequency at which words will be ignored which do not occur in at least this
		/// many docs.
		/// </returns>
		public int GetMinDocFreq()
		{
			return minDocFreq;
		}

		/// <summary> Sets the frequency at which words will be ignored which do not occur in at least this
		/// many docs.
		/// 
		/// </summary>
		/// <param name="minDocFreq">the frequency at which words will be ignored which do not occur in at
		/// least this many docs.
		/// </param>
		public void SetMinDocFreq(int minDocFreq)
		{
			this.minDocFreq = minDocFreq;
		}

		/// <summary> Returns whether to boost terms in query based on "score" or not. The default is
		/// {@link #DEFAULT_BOOST}.
		/// 
		/// </summary>
		/// <returns> whether to boost terms in query based on "score" or not.
		/// </returns>
		/// <seealso cref="#SetBoost">
		/// </seealso>
		public bool IsBoost()
		{
			return boost;
		}

		/// <summary> Sets whether to boost terms in query based on "score" or not.
		/// 
		/// </summary>
		/// <param name="boost">true to boost terms in query based on "score", false otherwise.
		/// </param>
		/// <seealso cref="#isBoost">
		/// </seealso>
		public void SetBoost(bool boost)
		{
			this.boost = boost;
		}

		/// <summary> Returns the field names that will be used when generating the 'More Like This' query.
		/// The default field names that will be used is {@link #DEFAULT_FIELD_NAMES}.
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

		/// <summary> Returns the minimum word length below which words will be ignored. Set this to 0 for no
		/// minimum word length. The default is {@link #DEFAULT_MIN_WORD_LENGTH}.
		/// 
		/// </summary>
		/// <returns> the minimum word length below which words will be ignored.
		/// </returns>
		public int GetMinWordLen()
		{
			return minWordLen;
		}

		/// <summary> Sets the minimum word length below which words will be ignored.
		/// 
		/// </summary>
		/// <param name="minWordLen">the minimum word length below which words will be ignored.
		/// </param>
		public void SetMinWordLen(int minWordLen)
		{
			this.minWordLen = minWordLen;
		}

		/// <summary> Returns the maximum word length above which words will be ignored. Set this to 0 for no
		/// maximum word length. The default is {@link #DEFAULT_MAX_WORD_LENGTH}.
		/// 
		/// </summary>
		/// <returns> the maximum word length above which words will be ignored.
		/// </returns>
		public int GetMaxWordLen()
		{
			return maxWordLen;
		}

		/// <summary> Sets the maximum word length above which words will be ignored.
		/// 
		/// </summary>
		/// <param name="maxWordLen">the maximum word length above which words will be ignored.
		/// </param>
		public void SetMaxWordLen(int maxWordLen)
		{
			this.maxWordLen = maxWordLen;
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
		/// <seealso cref="StopFilter.makeStopSet()">
		/// </seealso>
		/// <seealso cref="#getStopWords">
		/// </seealso>
		public void SetStopWords(System.Collections.Hashtable stopWords)
		{
			this.stopWords = stopWords;
		}

		/// <summary> Get the current stop words being used.</summary>
		/// <seealso cref="#setStopWords">
		/// </seealso>
		public System.Collections.Hashtable GetStopWords()
		{
			return stopWords;
		}


		/// <summary> Returns the maximum number of query terms that will be included in any generated query.
		/// The default is {@link #DEFAULT_MAX_QUERY_TERMS}.
		/// 
		/// </summary>
		/// <returns> the maximum number of query terms that will be included in any generated query.
		/// </returns>
		public int GetMaxQueryTerms()
		{
			return maxQueryTerms;
		}

		/// <summary> Sets the maximum number of query terms that will be included in any generated query.
		/// 
		/// </summary>
		/// <param name="maxQueryTerms">the maximum number of query terms that will be included in any
		/// generated query.
		/// </param>
		public void SetMaxQueryTerms(int maxQueryTerms)
		{
			this.maxQueryTerms = maxQueryTerms;
		}

		/// <returns> The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
		/// </returns>
		/// <seealso cref="#DEFAULT_MAX_NUM_TOKENS_PARSED">
		/// </seealso>
		public int GetMaxNumTokensParsed()
		{
			return maxNumTokensParsed;
		}

		/// <param name="i">The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
		/// </param>
		public void SetMaxNumTokensParsed(int i)
		{
			maxNumTokensParsed = i;
		}




		/// <summary> Return a query that will return docs like the passed lucene document ID.
		/// 
		/// </summary>
		/// <param name="docNum">the documentID of the lucene doc to generate the 'More Like This" query for.
		/// </param>
		/// <returns> a query that will return docs like the passed lucene document ID.
		/// </returns>
		public Query Like(int docNum)
		{
			if (fieldNames == null)
			{
				// gather list of valid fields from lucene
				System.Collections.Generic.ICollection<string> fields = ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
				System.Collections.IEnumerator e = fields.GetEnumerator();
				fieldNames = new System.String[fields.Count];
				int index = 0;
				while (e.MoveNext())
					fieldNames[index++] = (System.String)e.Current;
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
				System.Collections.Generic.ICollection<string> fields = ir.GetFieldNames(IndexReader.FieldOption.INDEXED);
				System.Collections.IEnumerator e = fields.GetEnumerator();
				fieldNames = new System.String[fields.Count];
				int index = 0;
				while (e.MoveNext())
					fieldNames[index++] = (System.String)e.Current;
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
			return Like(new System.IO.StreamReader(((System.Net.HttpWebRequest)System.Net.WebRequest.Create(u)).GetResponse().GetResponseStream(), System.Text.Encoding.Default));
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
		public Query Like(System.IO.StreamReader r)
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
						bestScore = (float)((System.Single)ar[2]);
					}
					float myScore = (float)((System.Single)ar[2]);

					tq.Boost = myScore / bestScore;
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
		protected PriorityQueue<object[]> CreateQueue(System.Collections.IDictionary words)
		{
			// have collected all words in doc and their freqs
			int numDocs = ir.NumDocs();
			FreqQ res = new FreqQ(words.Count); // will order words by score

			System.Collections.IEnumerator it = words.Keys.GetEnumerator();
			while (it.MoveNext())
			{
				// for every word
				System.String word = (System.String)it.Current;

				int tf = ((Int)words[word]).x; // term freq in the source doc
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

				if (docFreq == 0)
				{
					continue; // index update problem?
				}

				float idf = similarity.Idf(docFreq, numDocs);
				float score = tf * idf;

				// only really need 1st 3 entries, other ones are for troubleshooting
				res.InsertWithOverflow(new System.Object[] { word, topField, (float)score, (float)idf, (System.Int32)docFreq, (System.Int32)tf });
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

		/// <summary> Find words for a more-like-this query former.
		/// 
		/// </summary>
		/// <param name="docNum">the id of the lucene document from which to find terms
		/// </param>
		protected virtual PriorityQueue<object[]> RetrieveTerms(int docNum)
		{
			System.Collections.IDictionary termFreqMap = new System.Collections.Hashtable();
			for (int i = 0; i < fieldNames.Length; i++)
			{
				System.String fieldName = fieldNames[i];
				var vector = ir.GetTermFreqVector(docNum, fieldName);

				// field does not store term vector info
				if (vector == null)
				{
					Document d = ir.Document(docNum);
					System.String[] text = d.GetValues(fieldName);
					if (text != null)
					{
						for (int j = 0; j < text.Length; j++)
						{
							var stringReader = new System.IO.StringReader(text[j]);
							AddTermFrequencies(stringReader, termFreqMap, fieldName);
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
		protected void AddTermFrequencies(System.Collections.IDictionary termFreqMap, ITermFreqVector vector)
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
				Int cnt = (Int)termFreqMap[term];
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
		protected void AddTermFrequencies(System.IO.TextReader r, System.Collections.IDictionary termFreqMap, System.String fieldName)
		{
			TokenStream ts = analyzer.TokenStream(fieldName, r);
			var termAtt = ts.AddAttribute<ITermAttribute>();
			int tokenCount = 0;
			while (ts.IncrementToken())
			{
				// for every token
				System.String word = termAtt.Term;
				tokenCount++;
				if (tokenCount > maxNumTokensParsed)
				{
					break;
				}
				if (IsNoiseWord(word))
				{
					continue;
				}

				// increment frequency
				Int cnt = (Int)termFreqMap[word];
				if (cnt == null)
				{
					termFreqMap[word] = new Int();
				}
				else
				{
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
		protected bool IsNoiseWord(System.String term)
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
		/// <li> The word (String)
		/// <li> The top field that this word comes from (String)
		/// <li> The score for this word (Float)
		/// <li> The IDF value (Float)
		/// <li> The frequency of this word in the index (Integer)
		/// <li> The frequency of this word in the source document (Integer)	 	 
		/// </ol>
		/// This is a somewhat "advanced" routine, and in general only the 1st entry in the array is of interest.
		/// This method is exposed so that you can identify the "interesting words" in a document.
		/// For an easier method to call see {@link #retrieveInterestingTerms retrieveInterestingTerms()}.
		/// 
		/// </summary>
		/// <param name="r">the reader that has the content of the document
		/// </param>
		/// <returns> the most intresting words in the document ordered by score, with the highest scoring, or best entry, first
		/// 
		/// </returns>
		/// <seealso cref="#retrieveInterestingTerms">
		/// </seealso>
		public PriorityQueue<object[]> RetrieveTerms(System.IO.StreamReader r)
		{
			System.Collections.IDictionary words = new System.Collections.Hashtable();
			for (int i = 0; i < fieldNames.Length; i++)
			{
				System.String fieldName = fieldNames[i];
				AddTermFrequencies(r, words, fieldName);
			}
			return CreateQueue(words);
		}

		/// <summary> Convenience routine to make it easy to return the most interesting words in a document.
		/// More advanced users will call {@link #RetrieveTerms(java.io.Reader) retrieveTerms()} directly.
		/// </summary>
		/// <param name="r">the source document
		/// </param>
		/// <returns> the most interesting words in the document
		/// 
		/// </returns>
		/// <seealso cref="#RetrieveTerms(java.io.Reader)">
		/// </seealso>
		/// <seealso cref="#setMaxQueryTerms">
		/// </seealso>
		public System.String[] RetrieveInterestingTerms(System.IO.StreamReader r)
		{
			System.Collections.ArrayList al = new System.Collections.ArrayList(maxQueryTerms);
			var pq = RetrieveTerms(r);
			System.Object cur;
			int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
			// we just want to return the top words
			while (((cur = pq.Pop()) != null) && lim-- > 0)
			{
				System.Object[] ar = (System.Object[])cur;
				al.Add(ar[0]); // the 1st entry is the interesting word
			}
			System.String[] res = new System.String[al.Count];
			// return (System.String[]) SupportClass.ICollectionSupport.ToArray(al, res);
			return (System.String[])al.ToArray(typeof(System.String));
		}

		/// <summary> PriorityQueue that orders words by score.</summary>
		private class FreqQ : PriorityQueue<object[]>
		{
			internal FreqQ(int s)
			{
				Initialize(s);
			}

			public override bool LessThan(object[] a, object[] b)
			{
				System.Object[] aa = (System.Object[])a;
				System.Object[] bb = (System.Object[])b;
				System.Single fa = (System.Single)aa[2];
				System.Single fb = (System.Single)bb[2];
				return (float)fa > (float)fb;
			}
		}

		/// <summary> Use for frequencies and to avoid renewing Integers.</summary>
		protected class Int
		{
			internal int x;

			internal Int()
			{
				x = 1;
			}
		}
	}
}