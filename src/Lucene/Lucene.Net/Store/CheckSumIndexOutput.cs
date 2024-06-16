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
using Lucene.Net.Support;
using CRC32 = Lucene.Net.Support.CRC32;

namespace Lucene.Net.Store
{
	
	/// <summary>Writes bytes through to a primary IndexOutput, computing
	/// checksum.  Note that you cannot use seek().
	/// </summary>
	public class ChecksumIndexOutput:IndexOutput
	{
		internal IndexOutput main;
		internal IChecksum digest;

	    private bool isDisposed;
		
		public ChecksumIndexOutput(IndexOutput main)
		{
			this.main = main;
			digest = new CRC32();
		}
		
		public override void  WriteByte(byte b)
		{
			digest.Update(b);
			main.WriteByte(b);
		}
		
		public override void  WriteBytes(byte[] b, int offset, int length)
		{
			digest.Update(b, offset, length);
			main.WriteBytes(b, offset, length);
		}

	    public virtual long Checksum
	    {
	        get { return digest.Value; }
	    }

	    public override void  Flush()
		{
			main.Flush();
		}

        protected override void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                main.Close();
            }

            isDisposed = true;
        }

	    public override long FilePointer
	    {
	        get { return main.FilePointer; }
	    }

	    public override void  Seek(long pos)
		{
			throw new System.SystemException("not allowed");
		}
		
		/// <summary> Starts but does not complete the commit of this file (=
		/// writing of the final checksum at the end).  After this
		/// is called must call <see cref="FinishCommit" /> and the
		/// <see cref="Dispose" /> to complete the commit.
		/// </summary>
		public virtual void  PrepareCommit()
		{
			long checksum = Checksum;
			// Intentionally write a mismatched checksum.  This is
			// because we want to 1) test, as best we can, that we
			// are able to write a long to the file, but 2) not
			// actually "commit" the file yet.  This (prepare
			// commit) is phase 1 of a two-phase commit.
			long pos = main.FilePointer;
			main.WriteLong(checksum - 1);
			main.Flush();
			main.Seek(pos);
		}
		
		/// <summary>See <see cref="PrepareCommit" /> </summary>
		public virtual void  FinishCommit()
		{
			main.WriteLong(Checksum);
		}

	    public override long Length
	    {
	        get { return main.Length; }
	    }
	}
}