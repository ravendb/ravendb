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
using Lucene.Net.Documents;
using Lucene.Net.Store;
using FieldInvertState = Lucene.Net.Index.FieldInvertState;
using Term = Lucene.Net.Index.Term;
using SmallFloat = Lucene.Net.Util.SmallFloat;
using IDFExplanation = Lucene.Net.Search.Explanation.IDFExplanation;

namespace Lucene.Net.Search
{

    /// <summary>Expert: Scoring API.
    /// <p/>Subclasses implement search scoring.
    /// 
    /// <p/>The score of query <c>q</c> for document <c>d</c> correlates to the
    /// cosine-distance or dot-product between document and query vectors in a
    /// <a href="http://en.wikipedia.org/wiki/Vector_Space_Model">
    /// Vector Space Model (VSM) of Information Retrieval</a>.
    /// A document whose vector is closer to the query vector in that model is scored higher.
    /// 
    /// The score is computed as follows:
    /// 
    /// <p/>
    /// <table cellpadding="1" cellspacing="0" border="1" align="center">
    /// <tr><td>
    /// <table cellpadding="1" cellspacing="0" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right" rowspan="1">
    /// score(q,d) &#160; = &#160;
    /// <A HREF="#formula_coord">coord(q,d)</A> &#160;&#183;&#160;
    /// <A HREF="#formula_queryNorm">queryNorm(q)</A> &#160;&#183;&#160;
    /// </td>
    /// <td valign="bottom" align="center" rowspan="1">
    /// <big><big><big>&#8721;</big></big></big>
    /// </td>
    /// <td valign="middle" align="right" rowspan="1">
    /// <big><big>(</big></big>
    /// <A HREF="#formula_tf">tf(t in d)</A> &#160;&#183;&#160;
    /// <A HREF="#formula_idf">idf(t)</A><sup>2</sup> &#160;&#183;&#160;
    /// <A HREF="#formula_termBoost">t.Boost</A>&#160;&#183;&#160;
    /// <A HREF="#formula_norm">norm(t,d)</A>
    /// <big><big>)</big></big>
    /// </td>
    /// </tr>
    /// <tr valigh="top">
    /// <td></td>
    /// <td align="center"><small>t in q</small></td>
    /// <td></td>
    /// </tr>
    /// </table>
    /// </td></tr>
    /// </table>
    /// 
    /// <p/> where
    /// <list type="bullet">
    /// <item>
    /// <A NAME="formula_tf"></A>
    /// <b>tf(t in d)</b>
    /// correlates to the term's <i>frequency</i>,
    /// defined as the number of times term <i>t</i> appears in the currently scored document <i>d</i>.
    /// Documents that have more occurrences of a given term receive a higher score.
    /// The default computation for <i>tf(t in d)</i> in
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.Tf(float)">DefaultSimilarity</see> is:
    /// 
    /// <br/>&#160;<br/>
    /// <table cellpadding="2" cellspacing="2" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right" rowspan="1">
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.Tf(float)">tf(t in d)</see> &#160; = &#160;
    /// </td>
    /// <td valign="top" align="center" rowspan="1">
    /// frequency<sup><big>&#189;</big></sup>
    /// </td>
    /// </tr>
    /// </table>
    /// <br/>&#160;<br/>
    /// </item>
    /// 
    /// <item>
    /// <A NAME="formula_idf"></A>
    /// <b>idf(t)</b> stands for Inverse Document Frequency. This value
    /// correlates to the inverse of <i>docFreq</i>
    /// (the number of documents in which the term <i>t</i> appears).
    /// This means rarer terms give higher contribution to the total score.
    /// The default computation for <i>idf(t)</i> in 
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.Idf(int, int)">DefaultSimilarity</see> is:
    /// 
    /// <br/>&#160;<br/>
    /// <table cellpadding="2" cellspacing="2" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right">
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.Idf(int, int)">idf(t)</see>&#160; = &#160;
    /// </td>
    /// <td valign="middle" align="center">
    /// 1 + log <big>(</big>
    /// </td>
    /// <td valign="middle" align="center">
    /// <table>
    /// <tr><td align="center"><small>numDocs</small></td></tr>
    /// <tr><td align="center">&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;</td></tr>
    /// <tr><td align="center"><small>docFreq+1</small></td></tr>
    /// </table>
    /// </td>
    /// <td valign="middle" align="center">
    /// <big>)</big>
    /// </td>
    /// </tr>
    /// </table>
    /// <br/>&#160;<br/>
    /// </item>
    /// 
    /// <item>
    /// <A NAME="formula_coord"></A>
    /// <b>coord(q,d)</b>
    /// is a score factor based on how many of the query terms are found in the specified document.
    /// Typically, a document that contains more of the query's terms will receive a higher score
    /// than another document with fewer query terms.
    /// This is a search time factor computed in 
    /// <see cref="Coord(int, int)">coord(q,d)</see>
    /// by the Similarity in effect at search time.
    /// <br/>&#160;<br/>
    /// </item>
    /// 
    /// <item><b>
    /// <A NAME="formula_queryNorm"></A>
    /// queryNorm(q)
    /// </b>
    /// is a normalizing factor used to make scores between queries comparable.
    /// This factor does not affect document ranking (since all ranked documents are multiplied by the same factor),
    /// but rather just attempts to make scores from different queries (or even different indexes) comparable.
    /// This is a search time factor computed by the Similarity in effect at search time.
    /// 
    /// The default computation in
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.QueryNorm(float)">DefaultSimilarity</see>
    /// is:
    /// <br/>&#160;<br/>
    /// <table cellpadding="1" cellspacing="0" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right" rowspan="1">
    /// queryNorm(q)  &#160; = &#160;
    /// <see cref="Lucene.Net.Search.DefaultSimilarity.QueryNorm(float)">queryNorm(sumOfSquaredWeights)</see>
    /// &#160; = &#160;
    /// </td>
    /// <td valign="middle" align="center" rowspan="1">
    /// <table>
    /// <tr><td align="center"><big>1</big></td></tr>
    /// <tr><td align="center"><big>
    /// &#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;&#8211;
    /// </big></td></tr>
    /// <tr><td align="center">sumOfSquaredWeights<sup><big>&#189;</big></sup></td></tr>
    /// </table>
    /// </td>
    /// </tr>
    /// </table>
    /// <br/>&#160;<br/>
    /// 
    /// The sum of squared weights (of the query terms) is
    /// computed by the query <see cref="Lucene.Net.Search.Weight" /> object.
    /// For example, a <see cref="Lucene.Net.Search.BooleanQuery">boolean query</see>
    /// computes this value as:
    /// 
    /// <br/>&#160;<br/>
    /// <table cellpadding="1" cellspacing="0" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right" rowspan="1">
    /// <see cref="Lucene.Net.Search.Weight.GetSumOfSquaredWeights">GetSumOfSquaredWeights</see> &#160; = &#160;
    /// <see cref="Lucene.Net.Search.Query.Boost">q.Boost</see> <sup><big>2</big></sup>
    /// &#160;&#183;&#160;
    /// </td>
    /// <td valign="bottom" align="center" rowspan="1">
    /// <big><big><big>&#8721;</big></big></big>
    /// </td>
    /// <td valign="middle" align="right" rowspan="1">
    /// <big><big>(</big></big>
    /// <A HREF="#formula_idf">idf(t)</A> &#160;&#183;&#160;
    /// <A HREF="#formula_termBoost">t.Boost</A>
    /// <big><big>) <sup>2</sup> </big></big>
    /// </td>
    /// </tr>
    /// <tr valigh="top">
    /// <td></td>
    /// <td align="center"><small>t in q</small></td>
    /// <td></td>
    /// </tr>
    /// </table>
    /// <br/>&#160;<br/>
    /// 
    /// </item>
    /// 
    /// <item>
    /// <A NAME="formula_termBoost"></A>
    /// <b>t.Boost</b>
    /// is a search time boost of term <i>t</i> in the query <i>q</i> as
    /// specified in the query text
    /// (see <A HREF="../../../../../../queryparsersyntax.html#Boosting a Term">query syntax</A>),
    /// or as set by application calls to
    /// <see cref="Lucene.Net.Search.Query.Boost" />.
    /// Notice that there is really no direct API for accessing a boost of one term in a multi term query,
    /// but rather multi terms are represented in a query as multi
    /// <see cref="Lucene.Net.Search.TermQuery">TermQuery</see> objects,
    /// and so the boost of a term in the query is accessible by calling the sub-query
    /// <see cref="Lucene.Net.Search.Query.Boost" />.
    /// <br/>&#160;<br/>
    /// </item>
    /// 
    /// <item>
    /// <A NAME="formula_norm"></A>
    /// <b>norm(t,d)</b> encapsulates a few (indexing time) boost and length factors:
    /// 
    /// <list type="bullet">
    /// <item><b>Document boost</b> - set by calling 
    /// <see cref="Lucene.Net.Documents.Document.Boost">doc.Boost</see>
    /// before adding the document to the index.
    /// </item>
    /// <item><b>Field boost</b> - set by calling 
    /// <see cref="IFieldable.Boost">field.Boost</see>
    /// before adding the field to a document.
    /// </item>
    /// <item><see cref="LengthNorm(String, int)">LengthNorm(field)</see> - computed
    /// when the document is added to the index in accordance with the number of tokens
    /// of this field in the document, so that shorter fields contribute more to the score.
    /// LengthNorm is computed by the Similarity class in effect at indexing.
    /// </item>
    /// </list>
    /// 
    /// <p/>
    /// When a document is added to the index, all the above factors are multiplied.
    /// If the document has multiple fields with the same name, all their boosts are multiplied together:
    /// 
    /// <br/>&#160;<br/>
    /// <table cellpadding="1" cellspacing="0" border="0" align="center">
    /// <tr>
    /// <td valign="middle" align="right" rowspan="1">
    /// norm(t,d) &#160; = &#160;
    /// <see cref="Lucene.Net.Documents.Document.Boost">doc.Boost</see>
    /// &#160;&#183;&#160;
    /// <see cref="LengthNorm(String, int)">LengthNorm(field)</see>
    /// &#160;&#183;&#160;
    /// </td>
    /// <td valign="bottom" align="center" rowspan="1">
    /// <big><big><big>&#8719;</big></big></big>
    /// </td>
    /// <td valign="middle" align="right" rowspan="1">
    /// <see cref="IFieldable.Boost">field.Boost</see>
    /// </td>
    /// </tr>
    /// <tr valigh="top">
    /// <td></td>
    /// <td align="center"><small>field <i><b>f</b></i> in <i>d</i> named as <i><b>t</b></i></small></td>
    /// <td></td>
    /// </tr>
    /// </table>
    /// <br/>&#160;<br/>
    /// However the resulted <i>norm</i> value is <see cref="EncodeNorm(float)">encoded</see> as a single byte
    /// before being stored.
    /// At search time, the norm byte value is read from the index
    /// <see cref="Lucene.Net.Store.Directory">directory</see> and
    /// <see cref="DecodeNorm(byte)">decoded</see> back to a float <i>norm</i> value.
    /// This encoding/decoding, while reducing index size, comes with the price of
    /// precision loss - it is not guaranteed that decode(encode(x)) = x.
    /// For instance, decode(encode(0.89)) = 0.75.
    /// Also notice that search time is too late to modify this <i>norm</i> part of scoring, e.g. by
    /// using a different <see cref="Similarity" /> for search.
    /// <br/>&#160;<br/>
    /// </item>
    /// </list>
    /// 
    /// </summary>
    /// <seealso cref="Default">
    /// </seealso>
    /// <seealso cref="Lucene.Net.Index.IndexWriter.Similarity">
    /// </seealso>
    /// <seealso cref="Searcher.Similarity">
    /// </seealso>

        [Serializable]
    public abstract class Similarity
	{
	    protected Similarity()
		{
			InitBlock();
		}

        [Serializable]
        private class AnonymousClassIDFExplanation1:IDFExplanation
		{
			public AnonymousClassIDFExplanation1(int df, int max, float idf, Similarity enclosingInstance)
			{
				InitBlock(df, max, idf, enclosingInstance);
			}
			private void  InitBlock(int df, int max, float idf, Similarity enclosingInstance)
			{
				this.df = df;
				this.max = max;
				this.idf = idf;
				this.enclosingInstance = enclosingInstance;
			}
			private int df;
			private int max;
			private float idf;
			private Similarity enclosingInstance;
			public Similarity Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			//@Override
			public override System.String Explain()
			{
				return "idf(docFreq=" + df + ", maxDocs=" + max + ")";
			}
			//@Override

		    public override float Idf
		    {
		        get { return idf; }
		    }
		}

        [Serializable]
        private class AnonymousClassIDFExplanation3:IDFExplanation
		{
			public AnonymousClassIDFExplanation3(float fIdf, System.Text.StringBuilder exp, Similarity enclosingInstance)
			{
				InitBlock(fIdf, exp, enclosingInstance);
			}
			private void  InitBlock(float fIdf, System.Text.StringBuilder exp, Similarity enclosingInstance)
			{
				this.fIdf = fIdf;
				this.exp = exp;
				this.enclosingInstance = enclosingInstance;
			}
			private float fIdf;
			private System.Text.StringBuilder exp;
			private Similarity enclosingInstance;
			public Similarity Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			//@Override

		    public override float Idf
		    {
		        get { return fIdf; }
		    }

		    //@Override
			public override System.String Explain()
			{
				return exp.ToString();
			}
		}
		private void  InitBlock()
		{
			
		}

        /// <summary>The Similarity implementation used by default.</summary>
        private static Similarity defaultImpl = new DefaultSimilarity();
		public const int NO_DOC_ID_PROVIDED = - 1;

	    /// <summary>Gets or sets the default Similarity implementation 
	    /// used by indexing and search code.
	    /// <p/>This is initially an instance of <see cref="DefaultSimilarity" />.
	    /// </summary>
	    /// <seealso cref="Searcher.Similarity">
	    /// </seealso>
	    /// <seealso cref="Lucene.Net.Index.IndexWriter.SetSimilarity(Similarity)">
	    /// </seealso>
	    public static Similarity Default
	    {
	        get { return defaultImpl; }
	        set { defaultImpl = value; }
	    }

	    /// <summary>Cache of decoded bytes. </summary>
		private static readonly float[] NORM_TABLE = new float[256];
		
		/// <summary>Decodes a normalization factor stored in an index.</summary>
		/// <seealso cref="EncodeNorm(float)">
		/// </seealso>
		public static float DecodeNorm(byte b)
		{
			return NORM_TABLE[b & 0xFF]; // & 0xFF maps negative bytes to positive above 127
		}

	    /// <summary>Returns a table for decoding normalization bytes.</summary>
	    /// <seealso cref="EncodeNorm(float)">
	    /// </seealso>
	    public static float[] GetNormDecoder()
	    {
	        return NORM_TABLE;
	    }

	    /// <summary> Compute the normalization value for a field, given the accumulated
		/// state of term processing for this field (see <see cref="FieldInvertState" />).
		/// 
		/// <p/>Implementations should calculate a float value based on the field
		/// state and then return that value.
		/// 
		/// <p/>For backward compatibility this method by default calls
		/// <see cref="LengthNorm(String, int)" /> passing
		/// <see cref="FieldInvertState.Length" /> as the second argument, and
		/// then multiplies this value by <see cref="FieldInvertState.Boost" />.<p/>
		/// 
		/// <p/><b>WARNING</b>: This API is new and experimental and may
		/// suddenly change.<p/>
		/// 
		/// </summary>
		/// <param name="field">field name
		/// </param>
		/// <param name="state">current processing state for this field
		/// </param>
		/// <returns> the calculated float norm
		/// </returns>
		public virtual float ComputeNorm(System.String field, FieldInvertState state)
		{
			return (float) (state.Boost * LengthNorm(field, state.Length));
		}
		
		/// <summary>Computes the normalization value for a field given the total number of
		/// terms contained in a field.  These values, together with field boosts, are
		/// stored in an index and multipled into scores for hits on each field by the
		/// search code.
		/// 
		/// <p/>Matches in longer fields are less precise, so implementations of this
		/// method usually return smaller values when <c>numTokens</c> is large,
		/// and larger values when <c>numTokens</c> is small.
		/// 
		/// <p/>Note that the return values are computed under 
		/// <see cref="Lucene.Net.Index.IndexWriter.AddDocument(Lucene.Net.Documents.Document)" /> 
		/// and then stored using
		/// <see cref="EncodeNorm(float)" />.  
		/// Thus they have limited precision, and documents
		/// must be re-indexed if this method is altered.
		/// 
		/// </summary>
		/// <param name="fieldName">the name of the field
		/// </param>
		/// <param name="numTokens">the total number of tokens contained in fields named
		/// <i>fieldName</i> of <i>doc</i>.
		/// </param>
		/// <returns> a normalization factor for hits on this field of this document
		/// 
		/// </returns>
		/// <seealso cref="Lucene.Net.Documents.AbstractField.Boost" />
		public abstract float LengthNorm(System.String fieldName, int numTokens);
		
		/// <summary>Computes the normalization value for a query given the sum of the squared
		/// weights of each of the query terms.  This value is then multipled into the
		/// weight of each query term.
		/// 
		/// <p/>This does not affect ranking, but rather just attempts to make scores
		/// from different queries comparable.
		/// 
		/// </summary>
		/// <param name="sumOfSquaredWeights">the sum of the squares of query term weights
		/// </param>
		/// <returns> a normalization factor for query weights
		/// </returns>
		public abstract float QueryNorm(float sumOfSquaredWeights);
		
		/// <summary>Encodes a normalization factor for storage in an index.
		/// 
		/// <p/>The encoding uses a three-bit mantissa, a five-bit exponent, and
		/// the zero-exponent point at 15, thus
		/// representing values from around 7x10^9 to 2x10^-9 with about one
		/// significant decimal digit of accuracy.  Zero is also represented.
		/// Negative numbers are rounded up to zero.  Values too large to represent
		/// are rounded down to the largest representable value.  Positive values too
		/// small to represent are rounded up to the smallest positive representable
		/// value.
		/// 
		/// </summary>
		/// <seealso cref="Lucene.Net.Documents.AbstractField.Boost" />
		/// <seealso cref="Lucene.Net.Util.SmallFloat" />
		public static byte EncodeNorm(float f)
		{
			return (byte) SmallFloat.FloatToByte315(f);
		}
		
		
		/// <summary>Computes a score factor based on a term or phrase's frequency in a
		/// document.  This value is multiplied by the <see cref="Idf(int, int)" />
		/// factor for each term in the query and these products are then summed to
		/// form the initial score for a document.
		/// 
		/// <p/>Terms and phrases repeated in a document indicate the topic of the
		/// document, so implementations of this method usually return larger values
		/// when <c>freq</c> is large, and smaller values when <c>freq</c>
		/// is small.
		/// 
		/// <p/>The default implementation calls <see cref="Tf(float)" />.
		/// 
		/// </summary>
		/// <param name="freq">the frequency of a term within a document
		/// </param>
		/// <returns> a score factor based on a term's within-document frequency
		/// </returns>
		public virtual float Tf(int freq)
		{
			return Tf((float) freq);
		}
		
		/// <summary>Computes the amount of a sloppy phrase match, based on an edit distance.
		/// This value is summed for each sloppy phrase match in a document to form
		/// the frequency that is passed to <see cref="Tf(float)" />.
		/// 
		/// <p/>A phrase match with a small edit distance to a document passage more
		/// closely matches the document, so implementations of this method usually
		/// return larger values when the edit distance is small and smaller values
		/// when it is large.
		/// 
		/// </summary>
		/// <seealso cref="PhraseQuery.Slop" />
		/// <param name="distance">the edit distance of this sloppy phrase match </param>
		/// <returns> the frequency increment for this match </returns>
		public abstract float SloppyFreq(int distance);
		
		/// <summary>Computes a score factor based on a term or phrase's frequency in a
		/// document.  This value is multiplied by the <see cref="Idf(int, int)" />
		/// factor for each term in the query and these products are then summed to
		/// form the initial score for a document.
		/// 
		/// <p/>Terms and phrases repeated in a document indicate the topic of the
		/// document, so implementations of this method usually return larger values
		/// when <c>freq</c> is large, and smaller values when <c>freq</c>
		/// is small.
		/// 
		/// </summary>
		/// <param name="freq">the frequency of a term within a document
		/// </param>
		/// <returns> a score factor based on a term's within-document frequency
		/// </returns>
		public abstract float Tf(float freq);
		
		/// <summary> Computes a score factor for a simple term and returns an explanation
		/// for that score factor.
		/// 
		/// <p/>
		/// The default implementation uses:
		/// 
        /// <code>
		/// idf(searcher.docFreq(term), searcher.MaxDoc);
        /// </code>
		/// 
		/// Note that <see cref="Searcher.MaxDoc" /> is used instead of
		/// <see cref="Lucene.Net.Index.IndexReader.NumDocs()" /> because it is
		/// proportional to <see cref="Searcher.DocFreq(Term)" /> , i.e., when one is
		/// inaccurate, so is the other, and in the same direction.
		/// 
		/// </summary>
		/// <param name="term">the term in question
		/// </param>
		/// <param name="searcher">the document collection being searched
		/// </param>
		/// <returns> an IDFExplain object that includes both an idf score factor 
		/// and an explanation for the term.
		/// </returns>
		/// <throws>  IOException </throws>
		public virtual IDFExplanation IdfExplain(Term term, Searcher searcher, IState state)
		{
			int df = searcher.DocFreq(term, state);
			int max = searcher.MaxDoc;
			float idf2 = Idf(df, max);
			return new AnonymousClassIDFExplanation1(df, max, idf2, this);
		}
		
		/// <summary> Computes a score factor for a phrase.
		/// 
		/// <p/>
		/// The default implementation sums the idf factor for
		/// each term in the phrase.
		/// 
		/// </summary>
		/// <param name="terms">the terms in the phrase
		/// </param>
		/// <param name="searcher">the document collection being searched
		/// </param>
		/// <returns> an IDFExplain object that includes both an idf 
		/// score factor for the phrase and an explanation 
		/// for each term.
		/// </returns>
		/// <throws>  IOException </throws>
		public virtual IDFExplanation IdfExplain(ICollection<Term> terms, Searcher searcher, IState state)
		{
			int max = searcher.MaxDoc;
			float idf2 = 0.0f;
			System.Text.StringBuilder exp = new System.Text.StringBuilder();
            foreach (Term term in terms)
			{
				int df = searcher.DocFreq(term, state);
				idf2 += Idf(df, max);
				exp.Append(" ");
				exp.Append(term.Text);
				exp.Append("=");
				exp.Append(df);
			}
			float fIdf = idf2;
			return new AnonymousClassIDFExplanation3(fIdf, exp, this);
		}
		
		/// <summary>Computes a score factor based on a term's document frequency (the number
		/// of documents which contain the term).  This value is multiplied by the
		/// <see cref="Tf(int)" /> factor for each term in the query and these products are
		/// then summed to form the initial score for a document.
		/// 
		/// <p/>Terms that occur in fewer documents are better indicators of topic, so
		/// implementations of this method usually return larger values for rare terms,
		/// and smaller values for common terms.
		/// 
		/// </summary>
		/// <param name="docFreq">the number of documents which contain the term
		/// </param>
		/// <param name="numDocs">the total number of documents in the collection
		/// </param>
		/// <returns> a score factor based on the term's document frequency
		/// </returns>
		public abstract float Idf(int docFreq, int numDocs);
		
		/// <summary>Computes a score factor based on the fraction of all query terms that a
		/// document contains.  This value is multiplied into scores.
		/// 
		/// <p/>The presence of a large portion of the query terms indicates a better
		/// match with the query, so implementations of this method usually return
		/// larger values when the ratio between these parameters is large and smaller
		/// values when the ratio between them is small.
		/// 
		/// </summary>
		/// <param name="overlap">the number of query terms matched in the document
		/// </param>
		/// <param name="maxOverlap">the total number of terms in the query
		/// </param>
		/// <returns> a score factor based on term overlap with the query
		/// </returns>
		public abstract float Coord(int overlap, int maxOverlap);
		
		
		/// <summary> Calculate a scoring factor based on the data in the payload.  Overriding implementations
		/// are responsible for interpreting what is in the payload.  Lucene makes no assumptions about
		/// what is in the byte array.
		/// <p/>
		/// The default implementation returns 1.
		/// 
		/// </summary>
		/// <param name="docId">The docId currently being scored.  If this value is <see cref="NO_DOC_ID_PROVIDED" />, then it should be assumed that the PayloadQuery implementation does not provide document information
		/// </param>
		/// <param name="fieldName">The fieldName of the term this payload belongs to
		/// </param>
		/// <param name="start">The start position of the payload
		/// </param>
		/// <param name="end">The end position of the payload
		/// </param>
		/// <param name="payload">The payload byte array to be scored
		/// </param>
		/// <param name="offset">The offset into the payload array
		/// </param>
		/// <param name="length">The length in the array
		/// </param>
		/// <returns> An implementation dependent float to be used as a scoring factor
		/// 
		/// </returns>
		public virtual float ScorePayload(int docId, System.String fieldName, int start, int end, byte[] payload, int offset, int length)
		{
		    return 1;
		}

		static Similarity()
		{
			{
				for (int i = 0; i < 256; i++)
					NORM_TABLE[i] = SmallFloat.Byte315ToFloat((byte) i);
			}
		}
	}
}