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

namespace Lucene.Net.Store
{
	
	/// <summary> Used by MockRAMDirectory to create an output stream that
	/// will throw an IOException on fake disk full, track max
	/// disk space actually used, and maybe throw random
	/// IOExceptions.
	/// </summary>
	
	public class MockRAMOutputStream:RAMOutputStream
	{
		private MockRAMDirectory dir;
		private bool first = true;
		private System.String name;
		
		internal byte[] singleByte = new byte[1];
		
		/// <summary>Construct an empty output buffer. </summary>
		public MockRAMOutputStream(MockRAMDirectory dir, RAMFile f, System.String name):base(f)
		{
			this.dir = dir;
			this.name = name;
		}
		
		public override void  Close()
		{
			base.Close();
			
			// Now compute actual disk usage & track the maxUsedSize
			// in the MockRAMDirectory:
			long size = dir.GetRecomputedActualSizeInBytes();
			if (size > dir.maxUsedSize)
			{
				dir.maxUsedSize = size;
			}
		}
		
		public override void  Flush()
		{
			dir.MaybeThrowDeterministicException();
			base.Flush();
		}
		
		public override void  WriteByte(byte b)
		{
			singleByte[0] = b;
			WriteBytes(singleByte, 0, 1);
		}
		
		public override void  WriteBytes(byte[] b, int offset, int len)
		{
			long freeSpace = dir.maxSize - dir.SizeInBytes();
			long realUsage = 0;
			
			// If MockRAMDir crashed since we were opened, then
			// don't write anything:
			if (dir.crashed)
				throw new System.IO.IOException("MockRAMDirectory was crashed; cannot write to " + name);
			
			// Enforce disk full:
			if (dir.maxSize != 0 && freeSpace <= len)
			{
				// Compute the real disk free.  This will greatly slow
				// down our test but makes it more accurate:
				realUsage = dir.GetRecomputedActualSizeInBytes();
				freeSpace = dir.maxSize - realUsage;
			}
			
			if (dir.maxSize != 0 && freeSpace <= len)
			{
				if (freeSpace > 0 && freeSpace < len)
				{
					realUsage += freeSpace;
					base.WriteBytes(b, offset, (int) freeSpace);
				}
				if (realUsage > dir.maxUsedSize)
				{
					dir.maxUsedSize = realUsage;
				}
				throw new System.IO.IOException("fake disk full at " + dir.GetRecomputedActualSizeInBytes() + " bytes when writing " + name);
			}
			else
			{
				base.WriteBytes(b, offset, len);
			}
			
			dir.MaybeThrowDeterministicException();
			
			if (first)
			{
				// Maybe throw random exception; only do this on first
				// write to a new file:
				first = false;
				dir.MaybeThrowIOException();
			}
		}
	}
}