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
using System.Collections;
using System.Collections.Generic;
using Lucene.Net.Store;
using Lucene.Net.Support;

namespace Lucene.Net.Index
{
	
	/// <summary> This is a DocConsumer that gathers all fields under the
	/// same name, and calls per-field consumers to process field
	/// by field.  This class doesn't doesn't do any "real" work
	/// of its own: it just forwards the fields to a
	/// DocFieldConsumer.
	/// </summary>
	
	sealed class DocFieldProcessor : DocConsumer
	{
		
		internal DocumentsWriter docWriter;
		internal FieldInfos fieldInfos = new FieldInfos();
		internal DocFieldConsumer consumer;
		internal StoredFieldsWriter fieldsWriter;
		
		public DocFieldProcessor(DocumentsWriter docWriter, DocFieldConsumer consumer)
		{
			this.docWriter = docWriter;
			this.consumer = consumer;
			consumer.SetFieldInfos(fieldInfos);
			fieldsWriter = new StoredFieldsWriter(docWriter, fieldInfos);
		}
		
		public override void  CloseDocStore(SegmentWriteState state, IState s)
		{
			consumer.CloseDocStore(state, s);
			fieldsWriter.CloseDocStore(state, s);
		}
		
		public override void Flush(ICollection<DocConsumerPerThread> threads, SegmentWriteState state, IState s)
		{
			var childThreadsAndFields = new HashMap<DocFieldConsumerPerThread, ICollection<DocFieldConsumerPerField>>();
			foreach(DocConsumerPerThread thread in threads)
			{
                DocFieldProcessorPerThread perThread = (DocFieldProcessorPerThread)thread;
				childThreadsAndFields[perThread.consumer] = perThread.Fields();
				perThread.TrimFields(state);
			}
			fieldsWriter.Flush(state, s);
			consumer.Flush(childThreadsAndFields, state, s);
			
			// Important to save after asking consumer to flush so
			// consumer can alter the FieldInfo* if necessary.  EG,
			// FreqProxTermsWriter does this with
			// FieldInfo.storePayload.
			System.String fileName = state.SegmentFileName(IndexFileNames.FIELD_INFOS_EXTENSION);
			fieldInfos.Write(state.directory, fileName, s);
            state.flushedFiles.Add(fileName);
		}
		
		public override void  Abort()
		{
			fieldsWriter.Abort();
			consumer.Abort();
		}
		
		public override bool FreeRAM()
		{
			return consumer.FreeRAM();
		}
		
		public override DocConsumerPerThread AddThread(DocumentsWriterThreadState threadState)
		{
			return new DocFieldProcessorPerThread(threadState, this);
		}
	}
}