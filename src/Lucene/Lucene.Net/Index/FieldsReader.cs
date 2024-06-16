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
using System.IO;
using Lucene.Net.Support;
using Lucene.Net.Util;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using Lucene.Net.Documents;
using Lucene.Net.Store;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using BufferedIndexInput = Lucene.Net.Store.BufferedIndexInput;
using Directory = Lucene.Net.Store.Directory;
using IndexInput = Lucene.Net.Store.IndexInput;

namespace Lucene.Net.Index
{
	
	/// <summary> Class responsible for access to stored document fields.
	/// <p/>
	/// It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
	/// 
	/// </summary>
	public sealed class FieldsReader : ILuceneCloneable, IDisposable
	{
		private readonly FieldInfos fieldInfos;
		
		// The main fieldStream, used only for cloning.
		private readonly IndexInput cloneableFieldsStream;
		
		// This is a clone of cloneableFieldsStream used for reading documents.
		// It should not be cloned outside of a synchronized context.
		private readonly IndexInput fieldsStream;
		
		private readonly IndexInput cloneableIndexStream;
		private readonly IndexInput indexStream;
		private readonly int numTotalDocs;
		private readonly int size;
		private bool closed;
		private readonly int format;
		private readonly int formatSize;
		
		// The docID offset where our docs begin in the index
		// file.  This will be 0 if we have our own private file.
		private readonly int docStoreOffset;
		
		private readonly LightWeightThreadLocal<IndexInput> fieldsStreamTL = new LightWeightThreadLocal<IndexInput>();
		private readonly bool isOriginal = false;
		
		/// <summary>Returns a cloned FieldsReader that shares open
		/// IndexInputs with the original one.  It is the caller's
		/// job not to close the original FieldsReader until all
		/// clones are called (eg, currently SegmentReader manages
		/// this logic). 
		/// </summary>
		public System.Object Clone(IState state)
		{
			EnsureOpen();
			return new FieldsReader(fieldInfos, numTotalDocs, size, format, formatSize, docStoreOffset, cloneableFieldsStream, cloneableIndexStream, state);
		}
		
		// Used only by clone
		private FieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, int format, int formatSize, int docStoreOffset, IndexInput cloneableFieldsStream, IndexInput cloneableIndexStream, IState state)
		{
			this.fieldInfos = fieldInfos;
			this.numTotalDocs = numTotalDocs;
			this.size = size;
			this.format = format;
			this.formatSize = formatSize;
			this.docStoreOffset = docStoreOffset;
			this.cloneableFieldsStream = cloneableFieldsStream;
			this.cloneableIndexStream = cloneableIndexStream;
			fieldsStream = (IndexInput) cloneableFieldsStream.Clone(state);
			indexStream = (IndexInput) cloneableIndexStream.Clone(state);
		}
		
		public /*internal*/ FieldsReader(Directory d, String segment, FieldInfos fn, IState state) :this(d, segment, fn, BufferedIndexInput.BUFFER_SIZE, - 1, 0, state)
		{
		}
		
		internal FieldsReader(Directory d, System.String segment, FieldInfos fn, int readBufferSize, IState state) :this(d, segment, fn, readBufferSize, - 1, 0, state)
		{
		}
		
		internal FieldsReader(Directory d, System.String segment, FieldInfos fn, int readBufferSize, int docStoreOffset, int size, IState state)
		{
			bool success = false;
			isOriginal = true;
			try
			{
				fieldInfos = fn;
				
				cloneableFieldsStream = d.OpenInput(segment + "." + IndexFileNames.FIELDS_EXTENSION, readBufferSize, state);
				cloneableIndexStream = d.OpenInput(segment + "." + IndexFileNames.FIELDS_INDEX_EXTENSION, readBufferSize, state);
				
				// First version of fdx did not include a format
				// header, but, the first int will always be 0 in that
				// case
				int firstInt = cloneableIndexStream.ReadInt(state);
				format = firstInt == 0 ? 0 : firstInt;
				
				if (format > FieldsWriter.FORMAT_CURRENT)
					throw new CorruptIndexException("Incompatible format version: " + format + " expected " + FieldsWriter.FORMAT_CURRENT + " or lower");
				
				formatSize = format > FieldsWriter.FORMAT ? 4 : 0;
				
				if (format < FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES)
					cloneableFieldsStream.SetModifiedUTF8StringsMode();
				
				fieldsStream = (IndexInput) cloneableFieldsStream.Clone(state);
				
				long indexSize = cloneableIndexStream.Length(state) - formatSize;
				
				if (docStoreOffset != - 1)
				{
					// We read only a slice out of this shared fields file
					this.docStoreOffset = docStoreOffset;
					this.size = size;
					
					// Verify the file is long enough to hold all of our
					// docs
					System.Diagnostics.Debug.Assert(((int)(indexSize / 8)) >= size + this.docStoreOffset, "indexSize=" + indexSize + " size=" + size + " docStoreOffset=" + docStoreOffset);
				}
				else
				{
					this.docStoreOffset = 0;
					this.size = (int) (indexSize >> 3);
				}
				
				indexStream = (IndexInput) cloneableIndexStream.Clone(state);
				numTotalDocs = (int) (indexSize >> 3);
				success = true;
			}
			finally
			{
				// With lock-less commits, it's entirely possible (and
				// fine) to hit a FileNotFound exception above. In
				// this case, we want to explicitly close any subset
				// of things that were opened so that we don't have to
				// wait for a GC to do so.
				if (!success)
				{
					Dispose();
				}
			}
		}
		
		/// <throws>  AlreadyClosedException if this FieldsReader is closed </throws>
		internal void  EnsureOpen()
		{
			if (closed)
			{
				throw new AlreadyClosedException("this FieldsReader is closed");
			}
		}
		
		/// <summary> Closes the underlying <see cref="Lucene.Net.Store.IndexInput" /> streams, including any ones associated with a
		/// lazy implementation of a Field.  This means that the Fields values will not be accessible.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
        public void Dispose()
        {
            // Move to protected method if class becomes unsealed
            if (!closed)
            {
                if (fieldsStream != null)
                {
                    fieldsStream.Close();
                }
                if (isOriginal)
                {
                    if (cloneableFieldsStream != null)
                    {
                        cloneableFieldsStream.Close();
                    }
                    if (cloneableIndexStream != null)
                    {
                        cloneableIndexStream.Close();
                    }
                }
                if (indexStream != null)
                {
                    indexStream.Close();
                }
                fieldsStreamTL.Dispose();
                closed = true;
            }
        }
		
		public /*internal*/ int Size()
		{
			return size;
		}
		
		private void  SeekIndex(int docID, IState state)
		{
			indexStream.Seek(formatSize + (docID + docStoreOffset) * 8L, state);
		}
		
		internal bool CanReadRawDocs()
        {
            // Disable reading raw docs in 2.x format, because of the removal of compressed
            // fields in 3.0. We don't want rawDocs() to decode field bits to figure out
            // if a field was compressed, hence we enforce ordinary (non-raw) stored field merges
            // for <3.0 indexes.
			return format >= FieldsWriter.FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;
		}
		
		public /*internal*/ Document Doc(int n, FieldSelector fieldSelector, IState state)
		{
			SeekIndex(n, state);
			long position = indexStream.ReadLong(state);
			fieldsStream.Seek(position, state);
			
			var doc = new Document();
			int numFields = fieldsStream.ReadVInt(state);
			for (int i = 0; i < numFields; i++)
			{
				int fieldNumber = fieldsStream.ReadVInt(state);
				FieldInfo fi = fieldInfos.FieldInfo(fieldNumber);
				FieldSelectorResult acceptField = fieldSelector == null?FieldSelectorResult.LOAD:fieldSelector.Accept(fi.name);
				
				byte bits = fieldsStream.ReadByte(state);
				System.Diagnostics.Debug.Assert(bits <= FieldsWriter.FIELD_IS_COMPRESSED + FieldsWriter.FIELD_IS_TOKENIZED + FieldsWriter.FIELD_IS_BINARY);
				
				bool compressed = (bits & FieldsWriter.FIELD_IS_COMPRESSED) != 0;
			    System.Diagnostics.Debug.Assert(
			        (!compressed || (format < FieldsWriter.FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS)),
			        "compressed fields are only allowed in indexes of version <= 2.9");
				bool tokenize = (bits & FieldsWriter.FIELD_IS_TOKENIZED) != 0;
				bool binary = (bits & FieldsWriter.FIELD_IS_BINARY) != 0;
				//TODO: Find an alternative approach here if this list continues to grow beyond the
				//list of 5 or 6 currently here.  See Lucene 762 for discussion
				if (acceptField.Equals(FieldSelectorResult.LOAD))
				{
					AddField(doc, fi, binary, compressed, tokenize, state);
				}
				else if (acceptField.Equals(FieldSelectorResult.LOAD_AND_BREAK))
				{
					AddField(doc, fi, binary, compressed, tokenize, state);
					break; //Get out of this loop
				}
				else if (acceptField.Equals(FieldSelectorResult.LAZY_LOAD))
				{
					AddFieldLazy(doc, fi, binary, compressed, tokenize, state);
				}
				else if (acceptField.Equals(FieldSelectorResult.SIZE))
				{
					SkipField(binary, compressed, AddFieldSize(doc, fi, binary, compressed, state), state);
				}
				else if (acceptField.Equals(FieldSelectorResult.SIZE_AND_BREAK))
				{
					AddFieldSize(doc, fi, binary, compressed, state);
					break;
				}
				else
				{
					SkipField(binary, compressed, state);
				}
			}
			
			return doc;
		}
		
		/// <summary>Returns the length in bytes of each raw document in a
		/// contiguous range of length numDocs starting with
		/// startDocID.  Returns the IndexInput (the fieldStream),
		/// already seeked to the starting point for startDocID.
		/// </summary>
		internal IndexInput RawDocs(int[] lengths, int startDocID, int numDocs, IState state)
		{
			SeekIndex(startDocID, state);
			long startOffset = indexStream.ReadLong(state);
			long lastOffset = startOffset;
			int count = 0;
			while (count < numDocs)
			{
				long offset;
				int docID = docStoreOffset + startDocID + count + 1;
				System.Diagnostics.Debug.Assert(docID <= numTotalDocs);
				if (docID < numTotalDocs)
					offset = indexStream.ReadLong(state);
				else
					offset = fieldsStream.Length(state);
				lengths[count++] = (int) (offset - lastOffset);
				lastOffset = offset;
			}
			
			fieldsStream.Seek(startOffset, state);
			
			return fieldsStream;
		}
		
		/// <summary> Skip the field.  We still have to read some of the information about the field, but can skip past the actual content.
		/// This will have the most payoff on large fields.
		/// </summary>
		private void  SkipField(bool binary, bool compressed, IState state)
		{
			SkipField(binary, compressed, fieldsStream.ReadVInt(state), state);
		}
		
		private void  SkipField(bool binary, bool compressed, int toRead, IState state)
		{
			if (format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES || binary || compressed)
			{
				fieldsStream.Seek(fieldsStream.FilePointer(state) + toRead, state);
			}
			else
			{
				// We need to skip chars.  This will slow us down, but still better
				fieldsStream.SkipChars(toRead, state);
			}
		}
		
		private void  AddFieldLazy(Document doc, FieldInfo fi, bool binary, bool compressed, bool tokenize, IState state)
		{
			if (binary)
			{
				int toRead = fieldsStream.ReadVInt(state);
				long pointer = fieldsStream.FilePointer(state);
				//was: doc.add(new Fieldable(fi.name, b, Fieldable.Store.YES));
				doc.Add(new LazyField(this, fi.name, Field.Store.YES, toRead, pointer, binary, compressed));

				//Need to move the pointer ahead by toRead positions
				fieldsStream.Seek(pointer + toRead, state);
			}
			else
			{
				const Field.Store store = Field.Store.YES;
				Field.Index index = FieldExtensions.ToIndex(fi.isIndexed, tokenize);
				Field.TermVector termVector = FieldExtensions.ToTermVector(fi.storeTermVector, fi.storeOffsetWithTermVector, fi.storePositionWithTermVector);
				
				AbstractField f;
				if (compressed)
				{
					int toRead = fieldsStream.ReadVInt(state);
					long pointer = fieldsStream.FilePointer(state);
					f = new LazyField(this, fi.name, store, toRead, pointer, binary, compressed);
					//skip over the part that we aren't loading
					fieldsStream.Seek(pointer + toRead, state);
					f.OmitNorms = fi.omitNorms;
					f.OmitTermFreqAndPositions = fi.omitTermFreqAndPositions;
				}
				else
				{
					int length = fieldsStream.ReadVInt(state);
					long pointer = fieldsStream.FilePointer(state);
					//Skip ahead of where we are by the length of what is stored
                    if (format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES)
                    {
                        fieldsStream.Seek(pointer + length, state);
                    }
                    else
                    {
                        fieldsStream.SkipChars(length, state);
                    }
					f = new LazyField(this, fi.name, store, index, termVector, length, pointer, binary, compressed)
					    	{OmitNorms = fi.omitNorms, OmitTermFreqAndPositions = fi.omitTermFreqAndPositions};
				}

				doc.Add(f);
			}
		}

		private void AddField(Document doc, FieldInfo fi, bool binary, bool compressed, bool tokenize, IState state)
		{
			//we have a binary stored field, and it may be compressed
			if (binary)
			{
				int toRead = fieldsStream.ReadVInt(state);
				var b = new byte[toRead];
				fieldsStream.ReadBytes(b, 0, b.Length, state);
				doc.Add(compressed ? new Field(fi.name, Uncompress(b), Field.Store.YES) : new Field(fi.name, b, Field.Store.YES));
			}
			else
			{
				const Field.Store store = Field.Store.YES;
				Field.Index index = FieldExtensions.ToIndex(fi.isIndexed, tokenize);
				Field.TermVector termVector = FieldExtensions.ToTermVector(fi.storeTermVector, fi.storeOffsetWithTermVector, fi.storePositionWithTermVector);
				
				AbstractField f;
				if (compressed)
				{
					int toRead = fieldsStream.ReadVInt(state);
					
					var b = new byte[toRead];
					fieldsStream.ReadBytes(b, 0, b.Length, state);
					f = new Field(fi.name, false, System.Text.Encoding.GetEncoding("UTF-8").GetString(Uncompress(b)), store, index,
					              termVector) {OmitTermFreqAndPositions = fi.omitTermFreqAndPositions, OmitNorms = fi.omitNorms};
				}
				else
				{
					f = new Field(fi.name, false, fieldsStream.ReadString(state), store, index, termVector)
					    	{OmitTermFreqAndPositions = fi.omitTermFreqAndPositions, OmitNorms = fi.omitNorms};
				}

				doc.Add(f);
			}
		}
		
		// Add the size of field as a byte[] containing the 4 bytes of the integer byte size (high order byte first; char = 2 bytes)
		// Read just the size -- caller must skip the field content to continue reading fields
		// Return the size in bytes or chars, depending on field type
		private int AddFieldSize(Document doc, FieldInfo fi, bool binary, bool compressed, IState state)
		{
			int size = fieldsStream.ReadVInt(state), bytesize = binary || compressed?size:2 * size;
			var sizebytes = new byte[4];
			sizebytes[0] = (byte) (Number.URShift(bytesize, 24));
			sizebytes[1] = (byte) (Number.URShift(bytesize, 16));
			sizebytes[2] = (byte) (Number.URShift(bytesize, 8));
			sizebytes[3] = (byte) bytesize;
			doc.Add(new Field(fi.name, sizebytes, Field.Store.YES));
			return size;
		}

        /// <summary> A Lazy implementation of Fieldable that differs loading of fields until asked for, instead of when the Document is
        /// loaded.
        /// </summary>
        [Serializable]
        private sealed class LazyField : AbstractField
		{
			private void  InitBlock(FieldsReader enclosingInstance)
			{
				this.Enclosing_Instance = enclosingInstance;
			}

			private FieldsReader Enclosing_Instance { get; set; }

			private int toRead;
			private long pointer;
            [Obsolete("Only kept for backward-compatbility with <3.0 indexes. Will be removed in 4.0.")]
		    private readonly Boolean isCompressed;
			
			public LazyField(FieldsReader enclosingInstance, System.String name, Field.Store store, int toRead, long pointer, bool isBinary, bool isCompressed):base(name, store, Field.Index.NO, Field.TermVector.NO)
			{
				InitBlock(enclosingInstance);
				this.toRead = toRead;
				this.pointer = pointer;
				this.internalIsBinary = isBinary;
				if (isBinary)
					internalBinaryLength = toRead;
				lazy = true;
			    this.isCompressed = isCompressed;
			}
			
			public LazyField(FieldsReader enclosingInstance, System.String name, Field.Store store, Field.Index index, Field.TermVector termVector, int toRead, long pointer, bool isBinary, bool isCompressed):base(name, store, index, termVector)
			{
				InitBlock(enclosingInstance);
				this.toRead = toRead;
				this.pointer = pointer;
				this.internalIsBinary = isBinary;
				if (isBinary)
					internalBinaryLength = toRead;
				lazy = true;
			    this.isCompressed = isCompressed;
			}
			
			private IndexInput GetFieldStream(IState state)
			{
				IndexInput localFieldsStream = Enclosing_Instance.fieldsStreamTL.Get(state);
				if (localFieldsStream == null)
				{
					localFieldsStream = (IndexInput) Enclosing_Instance.cloneableFieldsStream.Clone(state);
					Enclosing_Instance.fieldsStreamTL.Set(localFieldsStream);
				}
				return localFieldsStream;
			}

		    /// <summary>The value of the field as a Reader, or null.  If null, the String value,
		    /// binary value, or TokenStream value is used.  Exactly one of StringValue(), 
		    /// ReaderValue(), GetBinaryValue(), and TokenStreamValue() must be set. 
		    /// </summary>
		    public override TextReader ReaderValue
		    {
		        get
		        {
		            Enclosing_Instance.EnsureOpen();
		            return null;
		        }
		    }

		    /// <summary>The value of the field as a TokenStream, or null.  If null, the Reader value,
		    /// String value, or binary value is used. Exactly one of StringValue(), 
		    /// ReaderValue(), GetBinaryValue(), and TokenStreamValue() must be set. 
		    /// </summary>
		    public override TokenStream TokenStreamValue
		    {
		        get
		        {
		            Enclosing_Instance.EnsureOpen();
		            return null;
		        }
		    }

		    /// <summary>The value of the field as a String, or null.  If null, the Reader value,
		    /// binary value, or TokenStream value is used.  Exactly one of StringValue(), 
		    /// ReaderValue(), GetBinaryValue(), and TokenStreamValue() must be set. 
		    /// </summary>
		    public override string StringValue(IState state)
		    {
		        Enclosing_Instance.EnsureOpen();
		        if (internalIsBinary)
		            return null;

		        if (fieldsData == null)
		        {
		        	IndexInput localFieldsStream = GetFieldStream(state);
		        	try
		        	{
		        		localFieldsStream.Seek(pointer, state);
		        		if (isCompressed)
		        		{
		        			var b = new byte[toRead];
		        			localFieldsStream.ReadBytes(b, 0, b.Length, state);
		        			fieldsData =
		        				System.Text.Encoding.GetEncoding("UTF-8").GetString(Enclosing_Instance.Uncompress(b));
		        		}
		        		else
		        		{
		        			if (Enclosing_Instance.format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES)
		        			{
		        				var bytes = new byte[toRead];
		        				localFieldsStream.ReadBytes(bytes, 0, toRead, state);
		        				fieldsData = System.Text.Encoding.GetEncoding("UTF-8").GetString(bytes);
		        			}
		        			else
		        			{
		        				//read in chars b/c we already know the length we need to read
		        				var chars = new char[toRead];
		        				localFieldsStream.ReadChars(chars, 0, toRead, state);
		        				fieldsData = new System.String(chars);
		        			}
		        		}
		        	}
		        	catch (System.IO.IOException e)
		        	{
		        		throw new FieldReaderException(e);
		        	}
		        }
		        return (System.String) fieldsData;
		    }

		    public long Pointer
		    {
		        get
		        {
		            Enclosing_Instance.EnsureOpen();
		            return pointer;
		        }
		        set
		        {
		            Enclosing_Instance.EnsureOpen();
		            this.pointer = value;
		        }
		    }

		    public int ToRead
		    {
		        get
		        {
		            Enclosing_Instance.EnsureOpen();
		            return toRead;
		        }
		        set
		        {
		            Enclosing_Instance.EnsureOpen();
		            this.toRead = value;
		        }
		    }

		    public override byte[] GetBinaryValue(byte[] result, IState state)
			{
				Enclosing_Instance.EnsureOpen();
				
				if (internalIsBinary)
				{
					if (fieldsData == null)
					{
						// Allocate new buffer if result is null or too small
						byte[] b;
						if (result == null || result.Length < toRead)
							b = new byte[toRead];
						else
							b = result;
						
						IndexInput localFieldsStream = GetFieldStream(state);
						
						// Throw this IOException since IndexReader.document does so anyway, so probably not that big of a change for people
						// since they are already handling this exception when getting the document
						try
						{
							localFieldsStream.Seek(pointer, state);
							localFieldsStream.ReadBytes(b, 0, toRead, state);
							fieldsData = isCompressed ? Enclosing_Instance.Uncompress(b) : b;
						}
						catch (IOException e)
						{
							throw new FieldReaderException(e);
						}
						
						internalbinaryOffset = 0;
						internalBinaryLength = toRead;
					}
					
					return (byte[]) fieldsData;
				}
		    	return null;
			}
		}
		
		private byte[] Uncompress(byte[] b)
		{
			try
			{
				return CompressionTools.Decompress(b);
			}
			catch (Exception e)
			{
				// this will happen if the field is not compressed
				throw new CorruptIndexException("field data are in wrong format: " + e, e);
			}
		}
	}
}