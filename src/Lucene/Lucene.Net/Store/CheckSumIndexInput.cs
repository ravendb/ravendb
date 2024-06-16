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

namespace Lucene.Net.Store
{
	
	/// <summary>Writes bytes through to a primary IndexOutput, computing
	/// checksum as it goes. Note that you cannot use seek(). 
	/// </summary>
	public class ChecksumIndexInput : IndexInput
	{
		internal IndexInput main;
		internal IChecksum digest;

	    private bool isDisposed;
		
		public ChecksumIndexInput(IndexInput main)
		{
			this.main = main;
            digest = new CRC32();
		}
		
		public override byte ReadByte(IState state)
		{
			byte b = main.ReadByte(state);
			digest.Update(b);
			return b;
		}
		
		public override void  ReadBytes(byte[] b, int offset, int len, IState state)
		{
			main.ReadBytes(b, offset, len, state);
			digest.Update(b, offset, len);
		}

	    public virtual long Checksum
	    {
	        get { return digest.Value; }
	    }

	    protected override void Dispose(bool disposing)
        {
            if (isDisposed) return;

            if (disposing)
            {
                if (main != null)
                {
                    main.Dispose();
                }
            }

            main = null;
            isDisposed = true;
        }

	    public override long FilePointer(IState state)
	    {
	        return main.FilePointer(state);
	    }

	    public override void  Seek(long pos, IState state)
		{
			throw new System.SystemException("not allowed");
		}
		
		public override long Length(IState state)
		{
			return main.Length(state);
		}
	}
}