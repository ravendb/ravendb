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

using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Store
{
	
	/// <summary>Test huge RAMFile with more than Integer.MAX_VALUE bytes. </summary>
	[TestFixture]
	public class TestHugeRamFile:LuceneTestCase
	{
		
		private static readonly long MAX_VALUE = (long) 2 * (long) System.Int32.MaxValue;
		
		/// <summary>Fake a huge ram file by using the same byte buffer for all 
		/// buffers under maxint. 
		/// </summary>
		[Serializable]
		private class DenseRAMFile:RAMFile
		{
			private long capacity = 0;
			private System.Collections.Hashtable singleBuffers = new System.Collections.Hashtable();
			public /*internal*/ override byte[] NewBuffer(int size)
			{
				capacity += size;
				if (capacity <= Lucene.Net.Store.TestHugeRamFile.MAX_VALUE)
				{
					// below maxint we reuse buffers
					byte[] buf = (byte[]) singleBuffers[(System.Int32) size];
					if (buf == null)
					{
						buf = new byte[size];
						//System.out.println("allocate: "+size);
						singleBuffers[(System.Int32) size] = buf;
					}
					return buf;
				}
				//System.out.println("allocate: "+size); System.out.flush();
				return new byte[size];
			}
		}
		
		/// <summary>Test huge RAMFile with more than Integer.MAX_VALUE bytes. (LUCENE-957) </summary>
		[Test]
		public virtual void  TestHugeFile()
		{
			DenseRAMFile f = new DenseRAMFile();
			// output part
			RAMOutputStream out_Renamed = new RAMOutputStream(f);
			byte[] b1 = new byte[RAMOutputStream.BUFFER_SIZE_ForNUnit];
			byte[] b2 = new byte[RAMOutputStream.BUFFER_SIZE_ForNUnit / 3];
			for (int i = 0; i < b1.Length; i++)
			{
				b1[i] = (byte) (i & 0x0007F);
			}
			for (int i = 0; i < b2.Length; i++)
			{
				b2[i] = (byte) (i & 0x0003F);
			}
			long n = 0;
			Assert.AreEqual(n, out_Renamed.Length(), "output length must match");
			while (n <= MAX_VALUE - b1.Length)
			{
				out_Renamed.WriteBytes(b1, 0, b1.Length);
				out_Renamed.Flush();
				n += b1.Length;
				Assert.AreEqual(n, out_Renamed.Length(), "output length must match");
			}
			//System.out.println("after writing b1's, length = "+out.length()+" (MAX_VALUE="+MAX_VALUE+")");
			int m = b2.Length;
			long L = 12;
			for (int j = 0; j < L; j++)
			{
				for (int i = 0; i < b2.Length; i++)
				{
					b2[i]++;
				}
				out_Renamed.WriteBytes(b2, 0, m);
				out_Renamed.Flush();
				n += m;
				Assert.AreEqual(n, out_Renamed.Length(), "output length must match");
			}
			out_Renamed.Close();
			// input part
			RAMInputStream in_Renamed = new RAMInputStream(f);
			Assert.AreEqual(n, in_Renamed.Length(), "input length must match");
			//System.out.println("input length = "+in.length()+" % 1024 = "+in.length()%1024);
			for (int j = 0; j < L; j++)
			{
				long loc = n - (L - j) * m;
				in_Renamed.Seek(loc / 3);
				in_Renamed.Seek(loc);
				for (int i = 0; i < m; i++)
				{
					byte bt = in_Renamed.ReadByte();
					byte expected = (byte) (1 + j + (i & 0x0003F));
					Assert.AreEqual(expected, bt, "must read same value that was written! j=" + j + " i=" + i);
				}
			}
		}
	}
}