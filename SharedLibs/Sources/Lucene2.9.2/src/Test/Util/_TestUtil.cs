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

using CheckIndex = Lucene.Net.Index.CheckIndex;
using ConcurrentMergeScheduler = Lucene.Net.Index.ConcurrentMergeScheduler;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MergeScheduler = Lucene.Net.Index.MergeScheduler;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Util
{
	
	public class _TestUtil
	{
		
		/// <summary>Returns temp dir, containing String arg in its name;
		/// does not create the directory. 
		/// </summary>
		public static System.IO.FileInfo GetTempDir(System.String desc)
		{
			System.String tempDir = System.IO.Path.GetTempPath();
			if (tempDir == null)
				throw new System.SystemException("java.io.tmpdir undefined, cannot run test");
			return new System.IO.FileInfo(System.IO.Path.Combine(tempDir, desc + "." + (new System.Random()).Next(System.Int32.MaxValue)));
		}
		
		public static void  RmDir(System.IO.FileInfo dir)
		{
			bool tmpBool;
			if (System.IO.File.Exists(dir.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(dir.FullName);
			if (tmpBool)
			{
				System.IO.FileInfo[] files = SupportClass.FileSupport.GetFiles(dir);
				for (int i = 0; i < files.Length; i++)
				{
					bool tmpBool2;
					if (System.IO.File.Exists(files[i].FullName))
					{
						System.IO.File.Delete(files[i].FullName);
						tmpBool2 = true;
					}
					else if (System.IO.Directory.Exists(files[i].FullName))
					{
						System.IO.Directory.Delete(files[i].FullName);
						tmpBool2 = true;
					}
					else
						tmpBool2 = false;
					if (!tmpBool2)
					{
						throw new System.IO.IOException("could not delete " + files[i]);
					}
				}
				bool tmpBool3;
				if (System.IO.File.Exists(dir.FullName))
				{
					System.IO.File.Delete(dir.FullName);
					tmpBool3 = true;
				}
				else if (System.IO.Directory.Exists(dir.FullName))
				{
					System.IO.Directory.Delete(dir.FullName);
					tmpBool3 = true;
				}
				else
					tmpBool3 = false;
				bool generatedAux = tmpBool3;
			}
		}
		
		public static void  RmDir(System.String dir)
		{
			RmDir(new System.IO.FileInfo(dir));
		}
		
		public static void  SyncConcurrentMerges(IndexWriter writer)
		{
			SyncConcurrentMerges(writer.GetMergeScheduler());
		}
		
		public static void  SyncConcurrentMerges(MergeScheduler ms)
		{
			if (ms is ConcurrentMergeScheduler)
				((ConcurrentMergeScheduler) ms).Sync();
		}
		
		/// <summary>This runs the CheckIndex tool on the index in.  If any
		/// issues are hit, a RuntimeException is thrown; else,
		/// true is returned. 
		/// </summary>
		public static bool CheckIndex(Directory dir)
		{
			System.IO.MemoryStream bos = new System.IO.MemoryStream(1024);
			
			CheckIndex checker = new CheckIndex(dir);
			checker.SetInfoStream(new System.IO.StreamWriter(bos));
			CheckIndex.Status indexStatus = checker.CheckIndex_Renamed_Method();
			if (indexStatus == null || indexStatus.clean == false)
			{
				System.Console.Out.WriteLine("CheckIndex failed");
				char[] tmpChar;
				byte[] tmpByte;
				tmpByte = bos.GetBuffer();
				tmpChar = new char[bos.Length];
				System.Array.Copy(tmpByte, 0, tmpChar, 0, tmpChar.Length);
				System.Console.Out.WriteLine(new System.String(tmpChar));
				throw new System.SystemException("CheckIndex failed");
			}
			else
				return true;
		}
		
		/// <summary>Use only for testing.</summary>
		/// <deprecated> -- in 3.0 we can use Arrays.toString
		/// instead 
		/// </deprecated>
		public static System.String ArrayToString(int[] array)
		{
			System.Text.StringBuilder buf = new System.Text.StringBuilder();
			buf.Append("[");
			for (int i = 0; i < array.Length; i++)
			{
				if (i > 0)
				{
					buf.Append(" ");
				}
				buf.Append(array[i]);
			}
			buf.Append("]");
			return buf.ToString();
		}
		
		/// <summary>Use only for testing.</summary>
		/// <deprecated> -- in 3.0 we can use Arrays.toString
		/// instead 
		/// </deprecated>
		public static System.String ArrayToString(System.Object[] array)
		{
			System.Text.StringBuilder buf = new System.Text.StringBuilder();
			buf.Append("[");
			for (int i = 0; i < array.Length; i++)
			{
				if (i > 0)
				{
					buf.Append(" ");
				}
				buf.Append(array[i]);
			}
			buf.Append("]");
			return buf.ToString();
		}
		
		public static int GetRandomSocketPort()
		{
			return 1024 + new System.Random().Next(64512);
		}
	}
}