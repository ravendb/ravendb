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
using Lucene.Net.Store;
using Directory = Lucene.Net.Store.Directory;
using IndexInput = Lucene.Net.Store.IndexInput;
using IndexOutput = Lucene.Net.Store.IndexOutput;

namespace Lucene.Net.Index
{
	
	
	/// <summary> Combines multiple files into a single compound file.
	/// The file format:<br/>
	/// <list type="bullet">
	/// <item>VInt fileCount</item>
	/// <item>{Directory}
	/// fileCount entries with the following structure:</item>
	/// <list type="bullet">
	/// <item>long dataOffset</item>
	/// <item>String fileName</item>
	/// </list>
	/// <item>{File Data}
	/// fileCount entries with the raw data of the corresponding file</item>
	/// </list>
	/// 
	/// The fileCount integer indicates how many files are contained in this compound
	/// file. The {directory} that follows has that many entries. Each directory entry
	/// contains a long pointer to the start of this file's data section, and a String
	/// with that file's name.
	/// </summary>
	public sealed class CompoundFileWriter : IDisposable
	{
		
		private sealed class FileEntry
		{
			/// <summary>source file </summary>
			internal System.String file;
			
			/// <summary>temporary holder for the start of directory entry for this file </summary>
			internal long directoryOffset;
			
			/// <summary>temporary holder for the start of this file's data section </summary>
			internal long dataOffset;
		}
		
		
		private readonly Directory directory;
		private readonly String fileName;
        private readonly HashSet<string> ids;
		private readonly LinkedList<FileEntry> entries;
		private bool merged = false;
		private readonly SegmentMerger.CheckAbort checkAbort;
		
		/// <summary>Create the compound stream in the specified file. The file name is the
		/// entire name (no extensions are added).
		/// </summary>
		/// <throws>  NullPointerException if <c>dir</c> or <c>name</c> is null </throws>
		public CompoundFileWriter(Directory dir, System.String name):this(dir, name, null)
		{
		}
		
		internal CompoundFileWriter(Directory dir, System.String name, SegmentMerger.CheckAbort checkAbort)
		{
			if (dir == null)
				throw new ArgumentNullException("dir");
			if (name == null)
				throw new ArgumentNullException("name");
			this.checkAbort = checkAbort;
			directory = dir;
			fileName = name;
            ids = new HashSet<string>();
			entries = new LinkedList<FileEntry>();
		}

	    /// <summary>Returns the directory of the compound file. </summary>
	    public Directory Directory
	    {
	        get { return directory; }
	    }

	    /// <summary>Returns the name of the compound file. </summary>
	    public string Name
	    {
	        get { return fileName; }
	    }

	    /// <summary>Add a source stream. <c>file</c> is the string by which the 
		/// sub-stream will be known in the compound stream.
		/// 
		/// </summary>
		/// <throws>  IllegalStateException if this writer is closed </throws>
		/// <throws>  NullPointerException if <c>file</c> is null </throws>
		/// <throws>  IllegalArgumentException if a file with the same name </throws>
		/// <summary>   has been added already
		/// </summary>
		public void  AddFile(String file)
		{
			if (merged)
				throw new InvalidOperationException("Can't add extensions after merge has been called");
			
			if (file == null)
				throw new ArgumentNullException("file");
			
            try
            {
                ids.Add(file);
            }
            catch (Exception)
            {
				throw new ArgumentException("File " + file + " already added");
            }

	    	var entry = new FileEntry {file = file};
	    	entries.AddLast(entry);
		}
		
        [Obsolete("Use Dispose() instead")]
		public void  Close()
		{
		    Dispose();
		}

        /// <summary>Merge files with the extensions added up to now.
        /// All files with these extensions are combined sequentially into the
        /// compound stream. After successful merge, the source files
        /// are deleted.
        /// </summary>
        /// <throws>  IllegalStateException if close() had been called before or </throws>
        /// <summary>   if no file has been added to this object
        /// </summary>
        public void Dispose()
        {
            // Extract into protected method if class ever becomes unsealed

            // TODO: Dispose shouldn't throw exceptions!
            if (merged)
                throw new SystemException("Merge already performed");

            if ((entries.Count == 0))
                throw new SystemException("No entries to merge have been defined");

            merged = true;

            // open the compound stream
            IndexOutput os = null;
            try
            {
                var state = StateHolder.Current.Value;
                os = directory.CreateOutput(fileName, state);

                // Write the number of entries
                os.WriteVInt(entries.Count);

                // Write the directory with all offsets at 0.
                // Remember the positions of directory entries so that we can
                // adjust the offsets later
                long totalSize = 0;
                foreach (FileEntry fe in entries)
                {
                    fe.directoryOffset = os.FilePointer;
                    os.WriteLong(0); // for now
                    os.WriteString(fe.file);
                    totalSize += directory.FileLength(fe.file, state);
                }

                // Pre-allocate size of file as optimization --
                // this can potentially help IO performance as
                // we write the file and also later during
                // searching.  It also uncovers a disk-full
                // situation earlier and hopefully without
                // actually filling disk to 100%:
                long finalLength = totalSize + os.FilePointer;
                os.SetLength(finalLength);

                // Open the files and copy their data into the stream.
                // Remember the locations of each file's data section.
                var buffer = new byte[16384];
                foreach (FileEntry fe in entries)
                {
                    fe.dataOffset = os.FilePointer;
                    CopyFile(fe, os, buffer, state);
                }

                // Write the data offsets into the directory of the compound stream
                foreach (FileEntry fe in entries)
                {
                    os.Seek(fe.directoryOffset);
                    os.WriteLong(fe.dataOffset);
                }

                System.Diagnostics.Debug.Assert(finalLength == os.Length);

                // Close the output stream. Set the os to null before trying to
                // close so that if an exception occurs during the close, the
                // finally clause below will not attempt to close the stream
                // the second time.
                IndexOutput tmp = os;
                os = null;
                tmp.Close();
            }
            finally
            {
                if (os != null)
                    try
                    {
                        os.Close();
                    }
                    catch (System.IO.IOException)
                    {
                    }
            }
        }

		
		/// <summary>Copy the contents of the file with specified extension into the
		/// provided output stream. Use the provided buffer for moving data
		/// to reduce memory allocation.
		/// </summary>
		private void  CopyFile(FileEntry source, IndexOutput os, byte[] buffer, IState state)
		{
			IndexInput isRenamed = null;
			try
			{
				long startPtr = os.FilePointer;
				
				isRenamed = directory.OpenInput(source.file, state);
				long length = isRenamed.Length(state);
				long remainder = length;
				int chunk = buffer.Length;
				
				while (remainder > 0)
				{
					var len = (int) Math.Min(chunk, remainder);
					isRenamed.ReadBytes(buffer, 0, len, false, state);
					os.WriteBytes(buffer, len);
					remainder -= len;
					if (checkAbort != null)
					// Roughly every 2 MB we will check if
					// it's time to abort
						checkAbort.Work(80, state);
				}
				
				// Verify that remainder is 0
				if (remainder != 0)
					throw new System.IO.IOException("Non-zero remainder length after copying: " + remainder + " (id: " + source.file + ", length: " + length + ", buffer size: " + chunk + ")");
				
				// Verify that the output length diff is equal to original file
				long endPtr = os.FilePointer;
				long diff = endPtr - startPtr;
				if (diff != length)
					throw new System.IO.IOException("Difference in the output file offsets " + diff + " does not match the original file length " + length);
			}
			finally
			{
				if (isRenamed != null)
					isRenamed.Close();
			}
		}
	}
}