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
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using Directory = Lucene.Net.Store.Directory;
using IndexInput = Lucene.Net.Store.IndexInput;
using IndexOutput = Lucene.Net.Store.IndexOutput;
using RAMOutputStream = Lucene.Net.Store.RAMOutputStream;

namespace Lucene.Net.Index
{
	
	sealed class FieldsWriter : IDisposable
	{
		internal const byte FIELD_IS_TOKENIZED = (0x1);
		internal const byte FIELD_IS_BINARY = (0x2);
        [Obsolete("Kept for backwards-compatibility with <3.0 indexes; will be removed in 4.0")]
		internal const byte FIELD_IS_COMPRESSED = (0x4);
		
		// Original format
		internal const int FORMAT = 0;
		
		// Changed strings to UTF8
		internal const int FORMAT_VERSION_UTF8_LENGTH_IN_BYTES = 1;
                 
        // Lucene 3.0: Removal of compressed fields
        internal static int FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS = 2;
		
		// NOTE: if you introduce a new format, make it 1 higher
		// than the current one, and always change this if you
		// switch to a new format!
        internal static readonly int FORMAT_CURRENT = FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;
		
		private readonly FieldInfos fieldInfos;
		
		private IndexOutput fieldsStream;
		
		private IndexOutput indexStream;
		
		private readonly bool doClose;
		
		internal FieldsWriter(Directory d, System.String segment, FieldInfos fn, IState state)
		{
			fieldInfos = fn;
			
			bool success = false;
			String fieldsName = segment + "." + IndexFileNames.FIELDS_EXTENSION;
			try
			{
				fieldsStream = d.CreateOutput(fieldsName, state);
				fieldsStream.WriteInt(FORMAT_CURRENT);
				success = true;
			}
			finally
			{
				if (!success)
				{
					try
					{
						Dispose();
					}
					catch (System.Exception)
					{
						// Suppress so we keep throwing the original exception
					}
					try
					{
						d.DeleteFile(fieldsName, state);
					}
					catch (System.Exception)
					{
						// Suppress so we keep throwing the original exception
					}
				}
			}
			
			success = false;
			String indexName = segment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION;
			try
			{
				indexStream = d.CreateOutput(indexName, state);
				indexStream.WriteInt(FORMAT_CURRENT);
				success = true;
			}
			finally
			{
				if (!success)
				{
					try
					{
						Dispose();
					}
					catch (System.IO.IOException)
					{
					}
					try
					{
						d.DeleteFile(fieldsName, state);
					}
					catch (System.Exception)
					{
						// Suppress so we keep throwing the original exception
					}
					try
					{
						d.DeleteFile(indexName, state);
					}
					catch (System.Exception)
					{
						// Suppress so we keep throwing the original exception
					}
				}
			}
			
			doClose = true;
		}
		
		internal FieldsWriter(IndexOutput fdx, IndexOutput fdt, FieldInfos fn)
		{
			fieldInfos = fn;
			fieldsStream = fdt;
			indexStream = fdx;
			doClose = false;
		}
		
		internal void  SetFieldsStream(IndexOutput stream)
		{
			this.fieldsStream = stream;
		}
		
		// Writes the contents of buffer into the fields stream
		// and adds a new entry for this document into the index
		// stream.  This assumes the buffer was already written
		// in the correct fields format.
		internal void  FlushDocument(int numStoredFields, RAMOutputStream buffer)
		{
			indexStream.WriteLong(fieldsStream.FilePointer);
			fieldsStream.WriteVInt(numStoredFields);
			buffer.WriteTo(fieldsStream);
		}
		
		internal void  SkipDocument()
		{
			indexStream.WriteLong(fieldsStream.FilePointer);
			fieldsStream.WriteVInt(0);
		}
		
		internal void  Flush()
		{
			indexStream.Flush();
			fieldsStream.Flush();
		}
		
		public void Dispose()
		{
            // Move to protected method if class becomes unsealed
			if (doClose)
			{
				try
				{
					if (fieldsStream != null)
					{
						try
						{
							fieldsStream.Close();
						}
						finally
						{
							fieldsStream = null;
						}
					}
				}
				catch (System.IO.IOException)
				{
					try
					{
						if (indexStream != null)
						{
							try
							{
								indexStream.Close();
							}
							finally
							{
								indexStream = null;
							}
						}
					}
					catch (System.IO.IOException)
					{
						// Ignore so we throw only first IOException hit
					}
					throw;
				}
				finally
				{
					if (indexStream != null)
					{
						try
						{
							indexStream.Close();
						}
						finally
						{
							indexStream = null;
						}
					}
				}
			}
		}
		
		internal void  WriteField(FieldInfo fi, IFieldable field, IState state)
		{
			fieldsStream.WriteVInt(fi.number);
			byte bits = 0;
			if (field.IsTokenized)
				bits |= FieldsWriter.FIELD_IS_TOKENIZED;
			if (field.IsBinary)
				bits |= FieldsWriter.FIELD_IS_BINARY;
			
			fieldsStream.WriteByte(bits);
			
			// compression is disabled for the current field
			if (field.IsBinary)
			{
				byte[] data = field.GetBinaryValue(state);
				int len = field.BinaryLength;
				int offset = field.BinaryOffset;
					
				fieldsStream.WriteVInt(len);
				fieldsStream.WriteBytes(data, offset, len);
			}
			else
			{
				fieldsStream.WriteString(field.StringValue(state));
			}
		}
		
		/// <summary>Bulk write a contiguous series of documents.  The
		/// lengths array is the length (in bytes) of each raw
		/// document.  The stream IndexInput is the
		/// fieldsStream from which we should bulk-copy all
		/// bytes. 
		/// </summary>
		internal void  AddRawDocuments(IndexInput stream, int[] lengths, int numDocs, IState state)
		{
			long position = fieldsStream.FilePointer;
			long start = position;
			for (int i = 0; i < numDocs; i++)
			{
				indexStream.WriteLong(position);
				position += lengths[i];
			}
			fieldsStream.CopyBytes(stream, position - start, state);
			System.Diagnostics.Debug.Assert(fieldsStream.FilePointer == position);
		}
		
		internal void  AddDocument(Document doc, IState state)
		{
			indexStream.WriteLong(fieldsStream.FilePointer);

			System.Collections.Generic.IList<IFieldable> fields = doc.GetFields();
			int storedCount = fields.Count(field => field.IsStored);
			fieldsStream.WriteVInt(storedCount);
			
			foreach(IFieldable field in fields)
			{
				if (field.IsStored)
					WriteField(fieldInfos.FieldInfo(field.Name), field, state);
			}
		}
	}
}