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

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using IndexInput = Lucene.Net.Store.IndexInput;
using IndexOutput = Lucene.Net.Store.IndexOutput;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/*
	Verify we can read the pre-2.1 file format, do searches
	against it, and add documents to it.*/
	
    [TestFixture]
	public class TestIndexFileDeleter:LuceneTestCase
	{
		[Test]
		public virtual void  TestDeleteLeftoverFiles()
		{
			
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			int i;
			for (i = 0; i < 35; i++)
			{
				AddDoc(writer, i);
			}
			writer.SetUseCompoundFile(false);
			for (; i < 45; i++)
			{
				AddDoc(writer, i);
			}
			writer.Close();
			
			// Delete one doc so we get a .del file:
			IndexReader reader = IndexReader.Open(dir);
			Term searchTerm = new Term("id", "7");
			int delCount = reader.DeleteDocuments(searchTerm);
			Assert.AreEqual(1, delCount, "didn't delete the right number of documents");
			
			// Set one norm so we get a .s0 file:
			reader.SetNorm(21, "content", (float) 1.5);
			reader.Close();
			
			// Now, artificially create an extra .del file & extra
			// .s0 file:
			System.String[] files = dir.ListAll();
			
			/*
			for(int j=0;j<files.length;j++) {
			System.out.println(j + ": " + files[j]);
			}
			*/
			
			// The numbering of fields can vary depending on which
			// JRE is in use.  On some JREs we see content bound to
			// field 0; on others, field 1.  So, here we have to
			// figure out which field number corresponds to
			// "content", and then set our expected file names below
			// accordingly:
			CompoundFileReader cfsReader = new CompoundFileReader(dir, "_2.cfs");
			FieldInfos fieldInfos = new FieldInfos(cfsReader, "_2.fnm");
			int contentFieldIndex = - 1;
			for (i = 0; i < fieldInfos.Size(); i++)
			{
				FieldInfo fi = fieldInfos.FieldInfo(i);
				if (fi.name_ForNUnit.Equals("content"))
				{
					contentFieldIndex = i;
					break;
				}
			}
			cfsReader.Close();
			Assert.IsTrue(contentFieldIndex != - 1, "could not locate the 'content' field number in the _2.cfs segment");
			
			System.String normSuffix = "s" + contentFieldIndex;
			
			// Create a bogus separate norms file for a
			// segment/field that actually has a separate norms file
			// already:
			CopyFile(dir, "_2_1." + normSuffix, "_2_2." + normSuffix);
			
			// Create a bogus separate norms file for a
			// segment/field that actually has a separate norms file
			// already, using the "not compound file" extension:
			CopyFile(dir, "_2_1." + normSuffix, "_2_2.f" + contentFieldIndex);
			
			// Create a bogus separate norms file for a
			// segment/field that does not have a separate norms
			// file already:
			CopyFile(dir, "_2_1." + normSuffix, "_1_1." + normSuffix);
			
			// Create a bogus separate norms file for a
			// segment/field that does not have a separate norms
			// file already using the "not compound file" extension:
			CopyFile(dir, "_2_1." + normSuffix, "_1_1.f" + contentFieldIndex);
			
			// Create a bogus separate del file for a
			// segment that already has a separate del file: 
			CopyFile(dir, "_0_1.del", "_0_2.del");
			
			// Create a bogus separate del file for a
			// segment that does not yet have a separate del file:
			CopyFile(dir, "_0_1.del", "_1_1.del");
			
			// Create a bogus separate del file for a
			// non-existent segment:
			CopyFile(dir, "_0_1.del", "_188_1.del");
			
			// Create a bogus segment file:
			CopyFile(dir, "_0.cfs", "_188.cfs");
			
			// Create a bogus fnm file when the CFS already exists:
			CopyFile(dir, "_0.cfs", "_0.fnm");
			
			// Create a deletable file:
			CopyFile(dir, "_0.cfs", "deletable");
			
			// Create some old segments file:
			CopyFile(dir, "segments_3", "segments");
			CopyFile(dir, "segments_3", "segments_2");
			
			// Create a bogus cfs file shadowing a non-cfs segment:
			CopyFile(dir, "_2.cfs", "_3.cfs");
			
			System.String[] filesPre = dir.ListAll();
			
			// Open & close a writer: it should delete the above 4
			// files and nothing more:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Close();
			
			System.String[] files2 = dir.ListAll();
			dir.Close();
			
			System.Array.Sort(files);
			System.Array.Sort(files2);
			
			System.Collections.Hashtable dif = DifFiles(files, files2);
			
			if (!SupportClass.CollectionsHelper.Equals(files, files2))
			{
				Assert.Fail("IndexFileDeleter failed to delete unreferenced extra files: should have deleted " + (filesPre.Length - files.Length) + " files but only deleted " + (filesPre.Length - files2.Length) + "; expected files:\n    " + AsString(files) + "\n  actual files:\n    " + AsString(files2) + "\ndif: " + SupportClass.CollectionsHelper.CollectionToString(dif));
			}
		}
		
		private static System.Collections.Hashtable DifFiles(System.String[] files1, System.String[] files2)
		{
			System.Collections.Hashtable set1 = new System.Collections.Hashtable();
			System.Collections.Hashtable set2 = new System.Collections.Hashtable();
			System.Collections.Hashtable extra = new System.Collections.Hashtable();
			for (int x = 0; x < files1.Length; x++)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(set1, files1[x]);
			}
			for (int x = 0; x < files2.Length; x++)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(set2, files2[x]);
			}
			System.Collections.IEnumerator i1 = set1.GetEnumerator();
			while (i1.MoveNext())
			{
				System.Object o = i1.Current;
				if (!set2.Contains(o))
				{
					SupportClass.CollectionsHelper.AddIfNotContains(extra, o);
				}
			}
			System.Collections.IEnumerator i2 = set2.GetEnumerator();
			while (i2.MoveNext())
			{
				System.Object o = i2.Current;
				if (!set1.Contains(o))
				{
					SupportClass.CollectionsHelper.AddIfNotContains(extra, o);
				}
			}
			return extra;
		}
		
		private System.String AsString(System.String[] l)
		{
			System.String s = "";
			for (int i = 0; i < l.Length; i++)
			{
				if (i > 0)
				{
					s += "\n    ";
				}
				s += l[i];
			}
			return s;
		}
		
		public virtual void  CopyFile(Directory dir, System.String src, System.String dest)
		{
			IndexInput in_Renamed = dir.OpenInput(src);
			IndexOutput out_Renamed = dir.CreateOutput(dest);
			byte[] b = new byte[1024];
			long remainder = in_Renamed.Length();
			while (remainder > 0)
			{
				int len = (int) System.Math.Min(b.Length, remainder);
				in_Renamed.ReadBytes(b, 0, len);
				out_Renamed.WriteBytes(b, len);
				remainder -= len;
			}
			in_Renamed.Close();
			out_Renamed.Close();
		}
		
		private void  AddDoc(IndexWriter writer, int id)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
			doc.Add(new Field("id", System.Convert.ToString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
			writer.AddDocument(doc);
		}
	}
}