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

/*
* Created on 28-Oct-2004
*/
using System;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using Token = Lucene.Net.Analysis.Token;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using Document = Lucene.Net.Documents.Document;
using IndexReader = Lucene.Net.Index.IndexReader;
using TermFreqVector = Lucene.Net.Index.TermFreqVector;
using TermPositionVector = Lucene.Net.Index.TermPositionVector;
using TermVectorOffsetInfo = Lucene.Net.Index.TermVectorOffsetInfo;

namespace Lucene.Net.Highlight
{
	
	/// <summary> Hides implementation issues associated with obtaining a TokenStream for use with
	/// the higlighter - can obtain from TermFreqVectors with offsets and (optionally) positions or
	/// from Analyzer class reparsing the stored content. 
	/// </summary>
	/// <author>  maharwood
	/// </author>
	public class TokenSources
	{
		public class StoredTokenStream : TokenStream
		{
			internal Token[] tokens;
			internal int currentToken = 0;
			internal StoredTokenStream(Token[] tokens)
			{
				this.tokens = tokens;
			}
			public override Token Next()
			{
				if (currentToken >= tokens.Length)
				{
					return null;
				}
				return tokens[currentToken++];
			}
		}
		private class AnonymousClassComparator : System.Collections.IComparer
		{
			public virtual int Compare(System.Object o1, System.Object o2)
			{
				Token t1 = (Token) o1;
				Token t2 = (Token) o2;
				if (t1.StartOffset() > t2.StartOffset())
					return 1;
				if (t1.StartOffset() < t2.StartOffset())
					return - 1;
				return 0;
			}
		}
		/// <summary> A convenience method that tries a number of approaches to getting a token stream.
		/// The cost of finding there are no termVectors in the index is minimal (1000 invocations still 
		/// registers 0 ms). So this "lazy" (flexible?) approach to coding is probably acceptable
		/// </summary>
		/// <param name="">reader
		/// </param>
		/// <param name="">docId
		/// </param>
		/// <param name="">field
		/// </param>
		/// <param name="">analyzer
		/// </param>
		/// <returns> null if field not stored correctly 
		/// </returns>
		/// <throws>  IOException </throws>
		public static TokenStream GetAnyTokenStream(IndexReader reader, int docId, System.String field, Analyzer analyzer)
		{
			TokenStream ts = null;
			
			TermFreqVector tfv = (TermFreqVector) reader.GetTermFreqVector(docId, field);
			if (tfv != null)
			{
				if (tfv is TermPositionVector)
				{
					ts = GetTokenStream((TermPositionVector) tfv);
				}
			}
			//No token info stored so fall back to analyzing raw content
			if (ts == null)
			{
				ts = GetTokenStream(reader, docId, field, analyzer);
			}
			return ts;
		}
		
		
		public static TokenStream GetTokenStream(TermPositionVector tpv)
		{
			//assumes the worst and makes no assumptions about token position sequences.
			return GetTokenStream(tpv, false);
		}
		/// <summary> Low level api.
		/// Returns a token stream or null if no offset info available in index.
		/// This can be used to feed the highlighter with a pre-parsed token stream 
		/// 
		/// In my tests the speeds to recreate 1000 token streams using this method are:
		/// - with TermVector offset only data stored - 420  milliseconds 
		/// - with TermVector offset AND position data stored - 271 milliseconds
		/// (nb timings for TermVector with position data are based on a tokenizer with contiguous
		/// positions - no overlaps or gaps)
		/// The cost of not using TermPositionVector to store
		/// pre-parsed content and using an analyzer to re-parse the original content: 
		/// - reanalyzing the original content - 980 milliseconds
		/// 
		/// The re-analyze timings will typically vary depending on -
		/// 1) The complexity of the analyzer code (timings above were using a 
		/// stemmer/lowercaser/stopword combo)
		/// 2) The  number of other fields (Lucene reads ALL fields off the disk 
		/// when accessing just one document field - can cost dear!)
		/// 3) Use of compression on field storage - could be faster cos of compression (less disk IO)
		/// or slower (more CPU burn) depending on the content.
		/// 
		/// </summary>
		/// <param name="">tpv
		/// </param>
		/// <param name="tokenPositionsGuaranteedContiguous">true if the token position numbers have no overlaps or gaps. If looking
		/// to eek out the last drops of performance, set to true. If in doubt, set to false.
		/// </param>
		public static TokenStream GetTokenStream(TermPositionVector tpv, bool tokenPositionsGuaranteedContiguous)
		{
			//an object used to iterate across an array of tokens
			//code to reconstruct the original sequence of Tokens
			System.String[] terms = tpv.GetTerms();
			int[] freq = tpv.GetTermFrequencies();
			int totalTokens = 0;
			for (int t = 0; t < freq.Length; t++)
			{
				totalTokens += freq[t];
			}
			Token[] tokensInOriginalOrder = new Token[totalTokens];
			System.Collections.ArrayList unsortedTokens = null;
			for (int t = 0; t < freq.Length; t++)
			{
				TermVectorOffsetInfo[] offsets = tpv.GetOffsets(t);
				if (offsets == null)
				{
					return null;
				}
				
				int[] pos = null;
				if (tokenPositionsGuaranteedContiguous)
				{
					//try get the token position info to speed up assembly of tokens into sorted sequence
					pos = tpv.GetTermPositions(t);
				}
				if (pos == null)
				{
					//tokens NOT stored with positions or not guaranteed contiguous - must add to list and sort later
					if (unsortedTokens == null)
					{
						unsortedTokens = new System.Collections.ArrayList();
					}
					for (int tp = 0; tp < offsets.Length; tp++)
					{
						unsortedTokens.Add(new Token(terms[t], offsets[tp].GetStartOffset(), offsets[tp].GetEndOffset()));
					}
				}
				else
				{
					//We have positions stored and a guarantee that the token position information is contiguous
					
					// This may be fast BUT wont work if Tokenizers used which create >1 token in same position or
					// creates jumps in position numbers - this code would fail under those circumstances
					
					//tokens stored with positions - can use this to index straight into sorted array
					for (int tp = 0; tp < pos.Length; tp++)
					{
						tokensInOriginalOrder[pos[tp]] = new Token(terms[t], offsets[tp].GetStartOffset(), offsets[tp].GetEndOffset());
					}
				}
			}
			//If the field has been stored without position data we must perform a sort        
			if (unsortedTokens != null)
			{
				tokensInOriginalOrder = (Token[]) unsortedTokens.ToArray(typeof(Token));
				Array.Sort(tokensInOriginalOrder, new AnonymousClassComparator());
			}
			return new StoredTokenStream(tokensInOriginalOrder);
		}
		
		public static TokenStream GetTokenStream(IndexReader reader, int docId, System.String field)
		{
			TermFreqVector tfv = (TermFreqVector) reader.GetTermFreqVector(docId, field);
			if (tfv == null)
			{
				throw new System.ArgumentException(field + " in doc #" + docId + "does not have any term position data stored");
			}
			if (tfv is TermPositionVector)
			{
				TermPositionVector tpv = (TermPositionVector) reader.GetTermFreqVector(docId, field);
				return GetTokenStream(tpv);
			}
			throw new System.ArgumentException(field + " in doc #" + docId + "does not have any term position data stored");
		}
		
		//convenience method
		public static TokenStream GetTokenStream(IndexReader reader, int docId, System.String field, Analyzer analyzer)
		{
			Document doc = reader.Document(docId);
			System.String contents = doc.Get(field);
			if (contents == null)
			{
				throw new System.ArgumentException("Field " + field + " in document #" + docId + " is not stored and cannot be analyzed");
			}
			return analyzer.TokenStream(field, new System.IO.StringReader(contents));
		}
	}
}