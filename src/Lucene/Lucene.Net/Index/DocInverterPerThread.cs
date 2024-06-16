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
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;
using TokenStream = Lucene.Net.Analysis.TokenStream;

namespace Lucene.Net.Index
{
	
	/// <summary>This is a DocFieldConsumer that inverts each field,
	/// separately, from a Document, and accepts a
	/// InvertedTermsConsumer to process those terms. 
	/// </summary>
	
	sealed class DocInverterPerThread : DocFieldConsumerPerThread
	{
		private void  InitBlock()
		{
			singleToken = new SingleTokenAttributeSource();
		}
		internal DocInverter docInverter;
		internal InvertedDocConsumerPerThread consumer;
		internal InvertedDocEndConsumerPerThread endConsumer;
		internal SingleTokenAttributeSource singleToken;
		
		internal class SingleTokenAttributeSource : AttributeSource
		{
			internal ITermAttribute termAttribute;
			internal IOffsetAttribute offsetAttribute;

            internal SingleTokenAttributeSource()
			{
                termAttribute = AddAttribute<ITermAttribute>();
				offsetAttribute = AddAttribute<IOffsetAttribute>();
			}
			
			public void  Reinit(System.String stringValue, int startOffset, int endOffset)
			{
				termAttribute.SetTermBuffer(stringValue);
				offsetAttribute.SetOffset(startOffset, endOffset);
			}
		}
		
		internal DocumentsWriter.DocState docState;
		
		internal FieldInvertState fieldState = new FieldInvertState();
		
		// Used to read a string value for a field
		internal ReusableStringReader stringReader = new ReusableStringReader();
		
		public DocInverterPerThread(DocFieldProcessorPerThread docFieldProcessorPerThread, DocInverter docInverter)
		{
			InitBlock();
			this.docInverter = docInverter;
			docState = docFieldProcessorPerThread.docState;
			consumer = docInverter.consumer.AddThread(this);
			endConsumer = docInverter.endConsumer.AddThread(this);
		}
		
		public override void  StartDocument()
		{
			consumer.StartDocument();
			endConsumer.StartDocument();
		}
		
		public override DocumentsWriter.DocWriter FinishDocument()
		{
			// TODO: allow endConsumer.finishDocument to also return
			// a DocWriter
			endConsumer.FinishDocument();
			return consumer.FinishDocument();
		}
		
		public override void  Abort()
		{
			try
			{
				consumer.Abort();
			}
			finally
			{
				endConsumer.Abort();
			}
		}
		
		public override DocFieldConsumerPerField AddField(FieldInfo fi)
		{
			return new DocInverterPerField(this, fi);
		}
	}
}