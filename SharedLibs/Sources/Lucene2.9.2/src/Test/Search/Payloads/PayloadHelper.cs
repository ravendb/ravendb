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

using NUnit.Framework;

using Lucene.Net.Analysis;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Payload = Lucene.Net.Index.Payload;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Similarity = Lucene.Net.Search.Similarity;
using English = Lucene.Net.Util.English;

namespace Lucene.Net.Search.Payloads
{
	
	/// <summary> 
	/// 
	/// 
	/// </summary>
	public class PayloadHelper
	{
		
		private byte[] payloadField = new byte[]{1};
		private byte[] payloadMultiField1 = new byte[]{2};
		private byte[] payloadMultiField2 = new byte[]{4};
		public const System.String NO_PAYLOAD_FIELD = "noPayloadField";
		public const System.String MULTI_FIELD = "multiField";
		public const System.String FIELD = "field";
		
		public class PayloadAnalyzer:Analyzer
		{
			public PayloadAnalyzer(PayloadHelper enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(PayloadHelper enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private PayloadHelper enclosingInstance;
			public PayloadHelper Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream result = new LowerCaseTokenizer(reader);
				result = new PayloadFilter(enclosingInstance, result, fieldName);
				return result;
			}
		}
		
		public class PayloadFilter:TokenFilter
		{
			private void  InitBlock(PayloadHelper enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private PayloadHelper enclosingInstance;
			public PayloadHelper Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String fieldName;
			internal int numSeen = 0;
			internal PayloadAttribute payloadAtt;
			
			public PayloadFilter(PayloadHelper enclosingInstance, TokenStream input, System.String fieldName):base(input)
			{
				InitBlock(enclosingInstance);
				this.fieldName = fieldName;
				payloadAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
			}
			
			public override bool IncrementToken()
			{
				
				if (input.IncrementToken())
				{
					if (fieldName.Equals(Lucene.Net.Search.Payloads.PayloadHelper.FIELD))
					{
						payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadField));
					}
					else if (fieldName.Equals(Lucene.Net.Search.Payloads.PayloadHelper.MULTI_FIELD))
					{
						if (numSeen % 2 == 0)
						{
							payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadMultiField1));
						}
						else
						{
							payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadMultiField2));
						}
						numSeen++;
					}
					return true;
				}
				return false;
			}
		}
		
		/// <summary> Sets up a RAMDirectory, and adds documents (using English.intToEnglish()) with two fields: field and multiField
		/// and analyzes them using the PayloadAnalyzer
		/// </summary>
		/// <param name="similarity">The Similarity class to use in the Searcher
		/// </param>
		/// <param name="numDocs">The num docs to add
		/// </param>
		/// <returns> An IndexSearcher
		/// </returns>
		/// <throws>  IOException </throws>
		public virtual IndexSearcher SetUp(Similarity similarity, int numDocs)
		{
			RAMDirectory directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true);
			writer.SetSimilarity(similarity);
			//writer.infoStream = System.out;
			for (int i = 0; i < numDocs; i++)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field(MULTI_FIELD, English.IntToEnglish(i) + "  " + English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field(NO_PAYLOAD_FIELD, English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			//writer.optimize();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			searcher.SetSimilarity(similarity);
			return searcher;
		}
	}
}