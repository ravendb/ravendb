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
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using NIOFSIndexInput = Lucene.Net.Store.NIOFSDirectory.NIOFSIndexInput;
using SimpleFSIndexInput = Lucene.Net.Store.SimpleFSDirectory.SimpleFSIndexInput;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Store
{
	
	[TestFixture]
	public class TestBufferedIndexInput:LuceneTestCase
	{
		
		private static void  WriteBytes(System.IO.FileInfo aFile, long size)
		{
			System.IO.Stream stream = null;
			try
			{
				stream = new System.IO.FileStream(aFile.FullName, System.IO.FileMode.Create);
				for (int i = 0; i < size; i++)
				{
					stream.WriteByte((byte) Byten(i));
				}
				stream.Flush();
			}
			finally
			{
				if (stream != null)
				{
					stream.Close();
				}
			}
		}
		
		private const long TEST_FILE_LENGTH = 1024 * 1024;
		
		// Call readByte() repeatedly, past the buffer boundary, and see that it
		// is working as expected.
		// Our input comes from a dynamically generated/ "file" - see
		// MyBufferedIndexInput below.
		[Test]
		public virtual void  TestReadByte()
		{
			MyBufferedIndexInput input = new MyBufferedIndexInput();
			for (int i = 0; i < BufferedIndexInput.BUFFER_SIZE * 10; i++)
			{
				Assert.AreEqual(input.ReadByte(), Byten(i));
			}
		}
		
		// Call readBytes() repeatedly, with various chunk sizes (from 1 byte to
		// larger than the buffer size), and see that it returns the bytes we expect.
		// Our input comes from a dynamically generated "file" -
		// see MyBufferedIndexInput below.
		[Test]
		public virtual void  TestReadBytes()
		{
			System.Random r = NewRandom();
			
			MyBufferedIndexInput input = new MyBufferedIndexInput();
			RunReadBytes(input, BufferedIndexInput.BUFFER_SIZE, r);
			
			// This tests the workaround code for LUCENE-1566 where readBytesInternal
			// provides a workaround for a JVM Bug that incorrectly raises a OOM Error
			// when a large byte buffer is passed to a file read.
			// NOTE: this does only test the chunked reads and NOT if the Bug is triggered.
			//final int tmpFileSize = 1024 * 1024 * 5;
			int inputBufferSize = 128;
			
            System.String tempDirectory = System.IO.Path.GetTempPath();

			System.IO.FileInfo tmpInputFile = new System.IO.FileInfo(System.IO.Path.Combine(tempDirectory, "IndexInput.tmpFile"));
			System.IO.File.Delete(tmpInputFile.FullName);
			WriteBytes(tmpInputFile, TEST_FILE_LENGTH);
			
			// run test with chunk size of 10 bytes
			RunReadBytesAndClose(new SimpleFSIndexInput(tmpInputFile, inputBufferSize, 10), inputBufferSize, r);
			// run test with chunk size of 100 MB - default
			RunReadBytesAndClose(new SimpleFSIndexInput(tmpInputFile, inputBufferSize), inputBufferSize, r);
			// run test with chunk size of 10 bytes
			//RunReadBytesAndClose(new NIOFSIndexInput(tmpInputFile, inputBufferSize, 10), inputBufferSize, r);    // {{Aroush-2.9}} suppressing this test since NIOFSIndexInput isn't ported
            System.Console.Out.WriteLine("Suppressing sub-test: 'RunReadBytesAndClose(new NIOFSIndexInput(tmpInputFile, inputBufferSize, 10), inputBufferSize, r);' since NIOFSIndexInput isn't ported");
			// run test with chunk size of 100 MB - default
			//RunReadBytesAndClose(new NIOFSIndexInput(tmpInputFile, inputBufferSize), inputBufferSize, r);     // {{Aroush-2.9}} suppressing this test since NIOFSIndexInput isn't ported
            System.Console.Out.WriteLine("Suppressing sub-test: 'RunReadBytesAndClose(new NIOFSIndexInput(tmpInputFile, inputBufferSize), inputBufferSize, r);' since NIOFSIndexInput isn't ported");
		}
		
		private void  RunReadBytesAndClose(IndexInput input, int bufferSize, System.Random r)
		{
			try
			{
				RunReadBytes(input, bufferSize, r);
			}
			finally
			{
				input.Close();
			}
		}
		
		private void  RunReadBytes(IndexInput input, int bufferSize, System.Random r)
		{
			
			int pos = 0;
			// gradually increasing size:
			for (int size = 1; size < bufferSize * 10; size = size + size / 200 + 1)
			{
				CheckReadBytes(input, size, pos);
				pos += size;
				if (pos >= TEST_FILE_LENGTH)
				{
					// wrap
					pos = 0;
					input.Seek(0L);
				}
			}
			// wildly fluctuating size:
			for (long i = 0; i < 1000; i++)
			{
				int size = r.Next(10000);
				CheckReadBytes(input, 1 + size, pos);
				pos += 1 + size;
				if (pos >= TEST_FILE_LENGTH)
				{
					// wrap
					pos = 0;
					input.Seek(0L);
				}
			}
			// constant small size (7 bytes):
			for (int i = 0; i < bufferSize; i++)
			{
				CheckReadBytes(input, 7, pos);
				pos += 7;
				if (pos >= TEST_FILE_LENGTH)
				{
					// wrap
					pos = 0;
					input.Seek(0L);
				}
			}
		}
		
		private byte[] buffer = new byte[10];
		
		private void  CheckReadBytes(IndexInput input, int size, int pos)
		{
			// Just to see that "offset" is treated properly in readBytes(), we
			// add an arbitrary offset at the beginning of the array
			int offset = size % 10; // arbitrary
			buffer = ArrayUtil.Grow(buffer, offset + size);
			Assert.AreEqual(pos, input.GetFilePointer());
			long left = TEST_FILE_LENGTH - input.GetFilePointer();
			if (left <= 0)
			{
				return ;
			}
			else if (left < size)
			{
				size = (int) left;
			}
			input.ReadBytes(buffer, offset, size);
			Assert.AreEqual(pos + size, input.GetFilePointer());
			for (int i = 0; i < size; i++)
			{
				Assert.AreEqual(Byten(pos + i), buffer[offset + i], "pos=" + i + " filepos=" + (pos + i));
			}
		}
		
		// This tests that attempts to readBytes() past an EOF will fail, while
		// reads up to the EOF will succeed. The EOF is determined by the
		// BufferedIndexInput's arbitrary length() value.
		[Test]
		public virtual void  TestEOF()
		{
			MyBufferedIndexInput input = new MyBufferedIndexInput(1024);
			// see that we can read all the bytes at one go:
			CheckReadBytes(input, (int) input.Length(), 0);
			// go back and see that we can't read more than that, for small and
			// large overflows:
			int pos = (int) input.Length() - 10;
			input.Seek(pos);
			CheckReadBytes(input, 10, pos);
			input.Seek(pos);
			try
			{
				CheckReadBytes(input, 11, pos);
				Assert.Fail("Block read past end of file");
			}
			catch (System.IO.IOException e)
			{
				/* success */
			}
			input.Seek(pos);
			try
			{
				CheckReadBytes(input, 50, pos);
				Assert.Fail("Block read past end of file");
			}
			catch (System.IO.IOException e)
			{
				/* success */
			}
			input.Seek(pos);
			try
			{
				CheckReadBytes(input, 100000, pos);
				Assert.Fail("Block read past end of file");
			}
			catch (System.IO.IOException e)
			{
				/* success */
			}
		}
		
		// byten emulates a file - byten(n) returns the n'th byte in that file.
		// MyBufferedIndexInput reads this "file".
		private static byte Byten(long n)
		{
			return (byte) (n * n % 256);
		}
		private class MyBufferedIndexInput:BufferedIndexInput
		{
			private long pos;
			private long len;
			public MyBufferedIndexInput(long len)
			{
				this.len = len;
				this.pos = 0;
			}
			public MyBufferedIndexInput():this(System.Int64.MaxValue)
			{
			}
			public override void  ReadInternal(byte[] b, int offset, int length)
			{
				for (int i = offset; i < offset + length; i++)
					b[i] = Lucene.Net.Store.TestBufferedIndexInput.Byten(pos++);
			}
			
			public override void  SeekInternal(long pos)
			{
				this.pos = pos;
			}
			
			public override void  Close()
			{
			}
			
			public override long Length()
			{
				return len;
			}
		}
		
		[Test]
		public virtual void  TestSetBufferSize()
		{
			System.IO.FileInfo indexDir = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testSetBufferSize"));
			MockFSDirectory dir = new MockFSDirectory(indexDir, NewRandom());
			try
			{
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				writer.SetUseCompoundFile(false);
				for (int i = 0; i < 37; i++)
				{
					Document doc = new Document();
					doc.Add(new Field("content", "aaa bbb ccc ddd" + i, Field.Store.YES, Field.Index.ANALYZED));
					doc.Add(new Field("id", "" + i, Field.Store.YES, Field.Index.ANALYZED));
					writer.AddDocument(doc);
				}
				writer.Close();
				
				dir.allIndexInputs.Clear();
				
				IndexReader reader = IndexReader.Open(dir);
				Term aaa = new Term("content", "aaa");
				Term bbb = new Term("content", "bbb");
				Term ccc = new Term("content", "ccc");
				Assert.AreEqual(37, reader.DocFreq(ccc));
				reader.DeleteDocument(0);
				Assert.AreEqual(37, reader.DocFreq(aaa));
				dir.tweakBufferSizes();
				reader.DeleteDocument(4);
				Assert.AreEqual(reader.DocFreq(bbb), 37);
				dir.tweakBufferSizes();
				
				IndexSearcher searcher = new IndexSearcher(reader);
				ScoreDoc[] hits = searcher.Search(new TermQuery(bbb), null, 1000).scoreDocs;
				dir.tweakBufferSizes();
				Assert.AreEqual(35, hits.Length);
				dir.tweakBufferSizes();
				hits = searcher.Search(new TermQuery(new Term("id", "33")), null, 1000).scoreDocs;
				dir.tweakBufferSizes();
				Assert.AreEqual(1, hits.Length);
				hits = searcher.Search(new TermQuery(aaa), null, 1000).scoreDocs;
				dir.tweakBufferSizes();
				Assert.AreEqual(35, hits.Length);
				searcher.Close();
				reader.Close();
			}
			finally
			{
				_TestUtil.RmDir(indexDir);
			}
		}
		
		private class MockFSDirectory:Directory
		{
			
			internal System.Collections.IList allIndexInputs = new System.Collections.ArrayList();
			
			internal System.Random rand;
			
			private Directory dir;
			
			public MockFSDirectory(System.IO.FileInfo path, System.Random rand)
			{
				this.rand = rand;
				lockFactory = new NoLockFactory();
				dir = new SimpleFSDirectory(path, null);
			}
			
			public override IndexInput OpenInput(System.String name)
			{
				return OpenInput(name, BufferedIndexInput.BUFFER_SIZE);
			}
			
			public virtual void  tweakBufferSizes()
			{
				System.Collections.IEnumerator it = allIndexInputs.GetEnumerator();
				//int count = 0;
				while (it.MoveNext())
				{
					BufferedIndexInput bii = (BufferedIndexInput) it.Current;
					int bufferSize = 1024 + (int) System.Math.Abs(rand.Next() % 32768);
					bii.SetBufferSize(bufferSize);
					//count++;
				}
				//System.out.println("tweak'd " + count + " buffer sizes");
			}
			
			public override IndexInput OpenInput(System.String name, int bufferSize)
			{
				// Make random changes to buffer size
				bufferSize = 1 + (int) System.Math.Abs(rand.Next() % 10);
				IndexInput f = dir.OpenInput(name, bufferSize);
				allIndexInputs.Add(f);
				return f;
			}
			
			public override IndexOutput CreateOutput(System.String name)
			{
				return dir.CreateOutput(name);
			}
			
			public override void  Close()
			{
				dir.Close();
			}
			
			public override void  DeleteFile(System.String name)
			{
				dir.DeleteFile(name);
			}
			public override void  TouchFile(System.String name)
			{
				dir.TouchFile(name);
			}
			public override long FileModified(System.String name)
			{
				return dir.FileModified(name);
			}
			public override bool FileExists(System.String name)
			{
				return dir.FileExists(name);
			}
			public override System.String[] List()
			{
				return dir.List();
			}
			public override System.String[] ListAll()
			{
				return dir.ListAll();
			}
			
			public override long FileLength(System.String name)
			{
				return dir.FileLength(name);
			}
			public override void  RenameFile(System.String from, System.String to)
			{
				dir.RenameFile(from, to);
			}
		}
	}
}