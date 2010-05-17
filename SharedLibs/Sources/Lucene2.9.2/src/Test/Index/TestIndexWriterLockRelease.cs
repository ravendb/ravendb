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

namespace Lucene.Net.Index
{
	
	/// <summary> This tests the patch for issue #LUCENE-715 (IndexWriter does not
	/// release its write lock when trying to open an index which does not yet
	/// exist).
	/// 
	/// </summary>
	/// <version>  $Id$
	/// </version>
	
    [TestFixture]
	public class TestIndexWriterLockRelease:LuceneTestCase
	{
		private System.IO.FileInfo __test_dir;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			if (this.__test_dir == null)
			{
				System.String tmp_dir = SupportClass.AppSettings.Get("java.io.tmpdir", "tmp");
				this.__test_dir = new System.IO.FileInfo(System.IO.Path.Combine(tmp_dir, "testIndexWriter"));
				
				bool tmpBool;
				if (System.IO.File.Exists(this.__test_dir.FullName))
					tmpBool = true;
				else
					tmpBool = System.IO.Directory.Exists(this.__test_dir.FullName);
				if (tmpBool)
				{
					throw new System.IO.IOException("test directory \"" + this.__test_dir.FullName + "\" already exists (please remove by hand)");
				}
				
				bool mustThrow = false;
				try
				{
					System.IO.Directory.CreateDirectory(this.__test_dir.FullName);
					if (!System.IO.Directory.Exists(this.__test_dir.FullName))
						mustThrow = true;
				}
				catch
				{
					mustThrow = true;
				}

				if (mustThrow)
					throw new System.IO.IOException("unable to create test directory \"" + this.__test_dir.FullName + "\"");
			}
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			if (this.__test_dir != null)
			{
				System.IO.FileInfo[] files = SupportClass.FileSupport.GetFiles(this.__test_dir);
				
				for (int i = 0; i < files.Length; ++i)
				{
					bool tmpBool;
					if (System.IO.File.Exists(files[i].FullName))
					{
						System.IO.File.Delete(files[i].FullName);
						tmpBool = true;
					}
					else if (System.IO.Directory.Exists(files[i].FullName))
					{
						System.IO.Directory.Delete(files[i].FullName);
						tmpBool = true;
					}
					else
						tmpBool = false;
					if (!tmpBool)
					{
						throw new System.IO.IOException("unable to remove file in test directory \"" + this.__test_dir.FullName + "\" (please remove by hand)");
					}
				}
				
				bool tmpBool2;
				if (System.IO.File.Exists(this.__test_dir.FullName))
				{
					System.IO.File.Delete(this.__test_dir.FullName);
					tmpBool2 = true;
				}
				else if (System.IO.Directory.Exists(this.__test_dir.FullName))
				{
					System.IO.Directory.Delete(this.__test_dir.FullName);
					tmpBool2 = true;
				}
				else
					tmpBool2 = false;
				if (!tmpBool2)
				{
					throw new System.IO.IOException("unable to remove test directory \"" + this.__test_dir.FullName + "\" (please remove by hand)");
				}
			}
		}
		
		[Test]
		public virtual void  TestIndexWriterLockRelease_Renamed()
		{
			IndexWriter im;
			
			try
			{
				im = new IndexWriter(this.__test_dir, new Lucene.Net.Analysis.Standard.StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			}
			catch (System.IO.FileNotFoundException e)
			{
				try
				{
					im = new IndexWriter(this.__test_dir, new Lucene.Net.Analysis.Standard.StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
				}
				catch (System.IO.FileNotFoundException e1)
				{
				}
			}
		}
	}
}