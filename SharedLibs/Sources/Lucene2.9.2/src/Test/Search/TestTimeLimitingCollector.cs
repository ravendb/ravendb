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
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using TimeExceededException = Lucene.Net.Search.TimeLimitingCollector.TimeExceededException;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests the {@link TimeLimitingCollector}.  This test checks (1) search
	/// correctness (regardless of timeout), (2) expected timeout behavior,
	/// and (3) a sanity test with multiple searching threads.
	/// </summary>
    [TestFixture]
	public class TestTimeLimitingCollector:LuceneTestCase
	{
		private class AnonymousClassThread:SupportClass.ThreadClass
		{
			public AnonymousClassThread(bool withTimeout, System.Collections.BitArray success, int num, TestTimeLimitingCollector enclosingInstance)
			{
				InitBlock(withTimeout, success, num, enclosingInstance);
			}
			private void  InitBlock(bool withTimeout, System.Collections.BitArray success, int num, TestTimeLimitingCollector enclosingInstance)
			{
				this.withTimeout = withTimeout;
				this.success = success;
				this.num = num;
				this.enclosingInstance = enclosingInstance;
			}
			private bool withTimeout;
			private System.Collections.BitArray success;
			private int num;
			private TestTimeLimitingCollector enclosingInstance;
			public TestTimeLimitingCollector Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			override public void  Run()
			{
				if (withTimeout)
				{
					Enclosing_Instance.DoTestTimeout(true, true);
				}
				else
				{
					Enclosing_Instance.DoTestSearch();
				}
				lock (success.SyncRoot)
				{
					success.Set(num, true);
				}
			}
		}
		private const int SLOW_DOWN = 47;
		private static readonly long TIME_ALLOWED = 17 * SLOW_DOWN; // so searches can find about 17 docs.
		
		// max time allowed is relaxed for multithreading tests. 
		// the multithread case fails when setting this to 1 (no slack) and launching many threads (>2000).  
		// but this is not a real failure, just noise.
		private const double MULTI_THREAD_SLACK = 7;
		
		private const int N_DOCS = 3000;
		private const int N_THREADS = 50;
		
		private Searcher searcher;
		private System.String FIELD_NAME = "body";
		private Query query;
		
		/*public TestTimeLimitingCollector(System.String name):base(name)
		{
		}*/
		
		/// <summary> initializes searcher with a document set</summary>
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			System.String[] docText = new System.String[]{"docThatNeverMatchesSoWeCanRequireLastDocCollectedToBeGreaterThanZero", "one blah three", "one foo three multiOne", "one foobar three multiThree", "blueberry pancakes", "blueberry pie", "blueberry strudel", "blueberry pizza"};
			Directory directory = new RAMDirectory();
			IndexWriter iw = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
			
			for (int i = 0; i < N_DOCS; i++)
			{
				Add(docText[i % docText.Length], iw);
			}
			iw.Close();
			searcher = new IndexSearcher(directory);
			
			System.String qtxt = "one";
			for (int i = 0; i < docText.Length; i++)
			{
				qtxt += (' ' + docText[i]); // large query so that search will be longer
			}
			QueryParser queryParser = new QueryParser(FIELD_NAME, new WhitespaceAnalyzer());
			query = queryParser.Parse(qtxt);
			
			// warm the searcher
			searcher.Search(query, null, 1000);
		}
		
		[TearDown]
		public override void  TearDown()
		{
			searcher.Close();
			base.TearDown();
		}
		
		private void  Add(System.String value_Renamed, IndexWriter iw)
		{
			Document d = new Document();
			d.Add(new Field(FIELD_NAME, value_Renamed, Field.Store.NO, Field.Index.ANALYZED));
			iw.AddDocument(d);
		}
		
		private void  Search(Collector collector)
		{
			searcher.Search(query, collector);
		}
		
		/// <summary> test search correctness with no timeout</summary>
        [Test]
		public virtual void  TestSearch()
		{
			DoTestSearch();
		}
		
		private void  DoTestSearch()
		{
			int totalResults = 0;
			int totalTLCResults = 0;
			try
			{
				MyHitCollector myHc = new MyHitCollector(this);
				Search(myHc);
				totalResults = myHc.HitCount();
				
				myHc = new MyHitCollector(this);
				long oneHour = 3600000;
				Collector tlCollector = CreateTimedCollector(myHc, oneHour, false);
				Search(tlCollector);
				totalTLCResults = myHc.HitCount();
			}
			catch (System.Exception e)
			{
				System.Console.Error.WriteLine(e.StackTrace);
				Assert.IsTrue(false, "Unexpected exception: " + e); //==fail
			}
			Assert.AreEqual(totalResults, totalTLCResults, "Wrong number of results!");
		}
		
		private Collector CreateTimedCollector(MyHitCollector hc, long timeAllowed, bool greedy)
		{
			TimeLimitingCollector res = new TimeLimitingCollector(hc, timeAllowed);
			res.SetGreedy(greedy); // set to true to make sure at least one doc is collected.
			return res;
		}
		
		/// <summary> Test that timeout is obtained, and soon enough!</summary>
        [Test]
		public virtual void  TestTimeoutGreedy()
		{
			DoTestTimeout(false, true);
		}
		
		/// <summary> Test that timeout is obtained, and soon enough!</summary>
        [Test]
		public virtual void  TestTimeoutNotGreedy()
		{
			DoTestTimeout(false, false);
		}
		
		private void  DoTestTimeout(bool multiThreaded, bool greedy)
		{
			// setup
			MyHitCollector myHc = new MyHitCollector(this);
			myHc.SetSlowDown(SLOW_DOWN);
			Collector tlCollector = CreateTimedCollector(myHc, TIME_ALLOWED, greedy);
			
			// search
			TimeExceededException timoutException = null;
			try
			{
				Search(tlCollector);
			}
			catch (TimeExceededException x)
			{
				timoutException = x;
			}
			catch (System.Exception e)
			{
				Assert.IsTrue(false, "Unexpected exception: " + e); //==fail
			}
			
			// must get exception
			Assert.IsNotNull(timoutException, "Timeout expected!");
			
			// greediness affect last doc collected
			int exceptionDoc = timoutException.GetLastDocCollected();
			int lastCollected = myHc.GetLastDocCollected();
			Assert.IsTrue(exceptionDoc > 0, "doc collected at timeout must be > 0!");
			if (greedy)
			{
				Assert.IsTrue(exceptionDoc == lastCollected, "greedy=" + greedy + " exceptionDoc=" + exceptionDoc + " != lastCollected=" + lastCollected);
				Assert.IsTrue(myHc.HitCount() > 0, "greedy, but no hits found!");
			}
			else
			{
				Assert.IsTrue(exceptionDoc > lastCollected, "greedy=" + greedy + " exceptionDoc=" + exceptionDoc + " not > lastCollected=" + lastCollected);
			}
			
			// verify that elapsed time at exception is within valid limits
			Assert.AreEqual(timoutException.GetTimeAllowed(), TIME_ALLOWED);
			// a) Not too early
			Assert.IsTrue(timoutException.GetTimeElapsed() > TIME_ALLOWED - TimeLimitingCollector.GetResolution(), "elapsed=" + timoutException.GetTimeElapsed() + " <= (allowed-resolution)=" + (TIME_ALLOWED - TimeLimitingCollector.GetResolution()));
			// b) Not too late.
			//    This part is problematic in a busy test system, so we just print a warning.
			//    We already verified that a timeout occurred, we just can't be picky about how long it took.
			if (timoutException.GetTimeElapsed() > MaxTime(multiThreaded))
			{
				System.Console.Out.WriteLine("Informative: timeout exceeded (no action required: most probably just " + " because the test machine is slower than usual):  " + "lastDoc=" + exceptionDoc + " ,&& allowed=" + timoutException.GetTimeAllowed() + " ,&& elapsed=" + timoutException.GetTimeElapsed() + " >= " + MaxTimeStr(multiThreaded));
			}
		}
		
		private long MaxTime(bool multiThreaded)
		{
			long res = 2 * TimeLimitingCollector.GetResolution() + TIME_ALLOWED + SLOW_DOWN; // some slack for less noise in this test
			if (multiThreaded)
			{
				res = (long) (res * MULTI_THREAD_SLACK); // larger slack  
			}
			return res;
		}
		
		private System.String MaxTimeStr(bool multiThreaded)
		{
			System.String s = "( " + "2*resolution +  TIME_ALLOWED + SLOW_DOWN = " + "2*" + TimeLimitingCollector.GetResolution() + " + " + TIME_ALLOWED + " + " + SLOW_DOWN + ")";
			if (multiThreaded)
			{
				s = MULTI_THREAD_SLACK + " * " + s;
			}
			return MaxTime(multiThreaded) + " = " + s;
		}
		
		/// <summary> Test timeout behavior when resolution is modified. </summary>
        [Test]
		public virtual void  TestModifyResolution()
		{
			try
			{
				// increase and test
				uint resolution = 20 * TimeLimitingCollector.DEFAULT_RESOLUTION; //400
				TimeLimitingCollector.SetResolution(resolution);
				Assert.AreEqual(resolution, TimeLimitingCollector.GetResolution());
				DoTestTimeout(false, true);
				// decrease much and test
				resolution = 5;
				TimeLimitingCollector.SetResolution(resolution);
				Assert.AreEqual(resolution, TimeLimitingCollector.GetResolution());
				DoTestTimeout(false, true);
				// return to default and test
				resolution = TimeLimitingCollector.DEFAULT_RESOLUTION;
				TimeLimitingCollector.SetResolution(resolution);
				Assert.AreEqual(resolution, TimeLimitingCollector.GetResolution());
				DoTestTimeout(false, true);
			}
			finally
			{
				TimeLimitingCollector.SetResolution(TimeLimitingCollector.DEFAULT_RESOLUTION);
			}
		}
		
		/// <summary> Test correctness with multiple searching threads.</summary>
        [Test]
		public virtual void  TestSearchMultiThreaded()
		{
			DoTestMultiThreads(false);
		}
		
		/// <summary> Test correctness with multiple searching threads.</summary>
        [Test]
		public virtual void  TestTimeoutMultiThreaded()
		{
			DoTestMultiThreads(true);
		}
		
		private void  DoTestMultiThreads(bool withTimeout)
		{
			SupportClass.ThreadClass[] threadArray = new SupportClass.ThreadClass[N_THREADS];
			System.Collections.BitArray success = new System.Collections.BitArray((N_THREADS % 64 == 0?N_THREADS / 64:N_THREADS / 64 + 1) * 64);
			for (int i = 0; i < threadArray.Length; ++i)
			{
				int num = i;
				threadArray[num] = new AnonymousClassThread(withTimeout, success, num, this);
			}
			for (int i = 0; i < threadArray.Length; ++i)
			{
				threadArray[i].Start();
			}
			for (int i = 0; i < threadArray.Length; ++i)
			{
				threadArray[i].Join();
			}
			Assert.AreEqual(N_THREADS, SupportClass.BitSetSupport.Cardinality(success), "some threads failed!");
		}
		
		// counting collector that can slow down at collect().
		private class MyHitCollector:Collector
		{
			public MyHitCollector(TestTimeLimitingCollector enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestTimeLimitingCollector enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTimeLimitingCollector enclosingInstance;
			public TestTimeLimitingCollector Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private System.Collections.BitArray bits = new System.Collections.BitArray(64);
			private int slowdown = 0;
			private int lastDocCollected = - 1;
			private int docBase = 0;
			
			/// <summary> amount of time to wait on each collect to simulate a long iteration</summary>
			public virtual void  SetSlowDown(int milliseconds)
			{
				slowdown = milliseconds;
			}
			
			public virtual int HitCount()
			{
				return SupportClass.BitSetSupport.Cardinality(bits);
			}
			
			public virtual int GetLastDocCollected()
			{
				return lastDocCollected;
			}
			
			public override void  SetScorer(Scorer scorer)
			{
				// scorer is not needed
			}
			
			public override void  Collect(int doc)
			{
				int docId = doc + docBase;
				if (slowdown > 0)
				{
					try
					{
						System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * slowdown));
					}
					catch (System.Threading.ThreadInterruptedException ie)
					{
						SupportClass.ThreadClass.Current().Interrupt();
						throw new System.SystemException("", ie);
					}
				}
				System.Diagnostics.Debug.Assert(docId >= 0, "base=" + docBase + " doc=" + doc);
                bits.Length = Math.Max(bits.Length, docId + 1);
				bits.Set(docId, true);
				lastDocCollected = docId;
			}
			
			public override void  SetNextReader(IndexReader reader, int base_Renamed)
			{
				docBase = base_Renamed;
			}
			
			public override bool AcceptsDocsOutOfOrder()
			{
				return false;
			}
		}
	}
}