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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using BaseTokenStreamTestCase = Lucene.Net.Analysis.BaseTokenStreamTestCase;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Occur = Lucene.Net.Search.BooleanClause.Occur;

namespace Lucene.Net.QueryParsers
{
	
	/// <summary> Tests QueryParser.</summary>
    [TestFixture]
	public class TestMultiFieldQueryParser:BaseTokenStreamTestCase
	{
		
		/// <summary>test stop words arsing for both the non static form, and for the 
		/// corresponding static form (qtxt, fields[]). 
		/// </summary>
		public virtual void  tesStopwordsParsing()
		{
			AssertStopQueryEquals("one", "b:one t:one");
			AssertStopQueryEquals("one stop", "b:one t:one");
			AssertStopQueryEquals("one (stop)", "b:one t:one");
			AssertStopQueryEquals("one ((stop))", "b:one t:one");
			AssertStopQueryEquals("stop", "");
			AssertStopQueryEquals("(stop)", "");
			AssertStopQueryEquals("((stop))", "");
		}
		
		// verify parsing of query using a stopping analyzer  
		private void  AssertStopQueryEquals(System.String qtxt, System.String expectedRes)
		{
			System.String[] fields = new System.String[]{"b", "t"};
			Occur[] occur = new Occur[]{Occur.SHOULD, Occur.SHOULD};
			TestQueryParser.QPTestAnalyzer a = new TestQueryParser.QPTestAnalyzer();
			MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, a);
			
			Query q = mfqp.Parse(qtxt);
			Assert.AreEqual(expectedRes, q.ToString());
			
			q = MultiFieldQueryParser.Parse(qtxt, fields, occur, a);
			Assert.AreEqual(expectedRes, q.ToString());
		}
		
		[Test]
		public virtual void  TestSimple()
		{
			System.String[] fields = new System.String[]{"b", "t"};
			MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new StandardAnalyzer());
			
			Query q = mfqp.Parse("one");
			Assert.AreEqual("b:one t:one", q.ToString());
			
			q = mfqp.Parse("one two");
			Assert.AreEqual("(b:one t:one) (b:two t:two)", q.ToString());
			
			q = mfqp.Parse("+one +two");
			Assert.AreEqual("+(b:one t:one) +(b:two t:two)", q.ToString());
			
			q = mfqp.Parse("+one -two -three");
			Assert.AreEqual("+(b:one t:one) -(b:two t:two) -(b:three t:three)", q.ToString());
			
			q = mfqp.Parse("one^2 two");
			Assert.AreEqual("((b:one t:one)^2.0) (b:two t:two)", q.ToString());
			
			q = mfqp.Parse("one~ two");
			Assert.AreEqual("(b:one~0.5 t:one~0.5) (b:two t:two)", q.ToString());
			
			q = mfqp.Parse("one~0.8 two^2");
			Assert.AreEqual("(b:one~0.8 t:one~0.8) ((b:two t:two)^2.0)", q.ToString());
			
			q = mfqp.Parse("one* two*");
			Assert.AreEqual("(b:one* t:one*) (b:two* t:two*)", q.ToString());
			
			q = mfqp.Parse("[a TO c] two");
			Assert.AreEqual("(b:[a TO c] t:[a TO c]) (b:two t:two)", q.ToString());
			
			q = mfqp.Parse("w?ldcard");
			Assert.AreEqual("b:w?ldcard t:w?ldcard", q.ToString());
			
			q = mfqp.Parse("\"foo bar\"");
			Assert.AreEqual("b:\"foo bar\" t:\"foo bar\"", q.ToString());
			
			q = mfqp.Parse("\"aa bb cc\" \"dd ee\"");
			Assert.AreEqual("(b:\"aa bb cc\" t:\"aa bb cc\") (b:\"dd ee\" t:\"dd ee\")", q.ToString());
			
			q = mfqp.Parse("\"foo bar\"~4");
			Assert.AreEqual("b:\"foo bar\"~4 t:\"foo bar\"~4", q.ToString());
			
			// LUCENE-1213: MultiFieldQueryParser was ignoring slop when phrase had a field.
			q = mfqp.Parse("b:\"foo bar\"~4");
			Assert.AreEqual("b:\"foo bar\"~4", q.ToString());
			
			// make sure that terms which have a field are not touched:
			q = mfqp.Parse("one f:two");
			Assert.AreEqual("(b:one t:one) f:two", q.ToString());
			
			// AND mode:
			mfqp.SetDefaultOperator(QueryParser.AND_OPERATOR);
			q = mfqp.Parse("one two");
			Assert.AreEqual("+(b:one t:one) +(b:two t:two)", q.ToString());
			q = mfqp.Parse("\"aa bb cc\" \"dd ee\"");
			Assert.AreEqual("+(b:\"aa bb cc\" t:\"aa bb cc\") +(b:\"dd ee\" t:\"dd ee\")", q.ToString());
		}
		
		[Test]
		public virtual void  TestBoostsSimple()
		{
			System.Collections.IDictionary boosts = new System.Collections.Hashtable();
			boosts["b"] = (float) 5;
			boosts["t"] = (float) 10;
			System.String[] fields = new System.String[]{"b", "t"};
			MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new StandardAnalyzer(), boosts);
			
			
			//Check for simple
			Query q = mfqp.Parse("one");
			Assert.AreEqual("b:one^5.0 t:one^10.0", q.ToString());
			
			//Check for AND
			q = mfqp.Parse("one AND two");
			Assert.AreEqual("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0)", q.ToString());
			
			//Check for OR
			q = mfqp.Parse("one OR two");
			Assert.AreEqual("(b:one^5.0 t:one^10.0) (b:two^5.0 t:two^10.0)", q.ToString());
			
			//Check for AND and a field
			q = mfqp.Parse("one AND two AND foo:test");
			Assert.AreEqual("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0) +foo:test", q.ToString());
			
			q = mfqp.Parse("one^3 AND two^4");
			Assert.AreEqual("+((b:one^5.0 t:one^10.0)^3.0) +((b:two^5.0 t:two^10.0)^4.0)", q.ToString());
		}
		
		[Test]
		public virtual void  TestStaticMethod1()
		{
			System.String[] fields = new System.String[]{"b", "t"};
			System.String[] queries = new System.String[]{"one", "two"};
			Query q = MultiFieldQueryParser.Parse(queries, fields, new StandardAnalyzer());
			Assert.AreEqual("b:one t:two", q.ToString());
			
			System.String[] queries2 = new System.String[]{"+one", "+two"};
			q = MultiFieldQueryParser.Parse(queries2, fields, new StandardAnalyzer());
			Assert.AreEqual("(+b:one) (+t:two)", q.ToString());
			
			System.String[] queries3 = new System.String[]{"one", "+two"};
			q = MultiFieldQueryParser.Parse(queries3, fields, new StandardAnalyzer());
			Assert.AreEqual("b:one (+t:two)", q.ToString());
			
			System.String[] queries4 = new System.String[]{"one +more", "+two"};
			q = MultiFieldQueryParser.Parse(queries4, fields, new StandardAnalyzer());
			Assert.AreEqual("(b:one +b:more) (+t:two)", q.ToString());
			
			System.String[] queries5 = new System.String[]{"blah"};
			try
			{
				q = MultiFieldQueryParser.Parse(queries5, fields, new StandardAnalyzer());
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception, array length differs
			}
			
			// check also with stop words for this static form (qtxts[], fields[]).
			TestQueryParser.QPTestAnalyzer stopA = new TestQueryParser.QPTestAnalyzer();
			
			System.String[] queries6 = new System.String[]{"((+stop))", "+((stop))"};
			q = MultiFieldQueryParser.Parse(queries6, fields, stopA);
			Assert.AreEqual("", q.ToString());
			
			System.String[] queries7 = new System.String[]{"one ((+stop)) +more", "+((stop)) +two"};
			q = MultiFieldQueryParser.Parse(queries7, fields, stopA);
			Assert.AreEqual("(b:one +b:more) (+t:two)", q.ToString());
		}
		
		[Test]
		public virtual void  TestStaticMethod2()
		{
			System.String[] fields = new System.String[]{"b", "t"};
			BooleanClause.Occur[] flags = new BooleanClause.Occur[]{BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
			Query q = MultiFieldQueryParser.Parse("one", fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+b:one -t:one", q.ToString());
			
			q = MultiFieldQueryParser.Parse("one two", fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+(b:one b:two) -(t:one t:two)", q.ToString());
			
			try
			{
				BooleanClause.Occur[] flags2 = new BooleanClause.Occur[]{BooleanClause.Occur.MUST};
				q = MultiFieldQueryParser.Parse("blah", fields, flags2, new StandardAnalyzer());
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception, array length differs
			}
		}
		
		[Test]
		public virtual void  TestStaticMethod2Old()
		{
			System.String[] fields = new System.String[]{"b", "t"};
			//int[] flags = {MultiFieldQueryParser.REQUIRED_FIELD, MultiFieldQueryParser.PROHIBITED_FIELD};
			BooleanClause.Occur[] flags = new BooleanClause.Occur[]{BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
			MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new StandardAnalyzer());
			
			Query q = MultiFieldQueryParser.Parse("one", fields, flags, new StandardAnalyzer()); //, fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+b:one -t:one", q.ToString());
			
			q = MultiFieldQueryParser.Parse("one two", fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+(b:one b:two) -(t:one t:two)", q.ToString());
			
			try
			{
				BooleanClause.Occur[] flags2 = new BooleanClause.Occur[]{BooleanClause.Occur.MUST};
				q = MultiFieldQueryParser.Parse("blah", fields, flags2, new StandardAnalyzer());
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception, array length differs
			}
		}
		
		[Test]
		public virtual void  TestStaticMethod3()
		{
			System.String[] queries = new System.String[]{"one", "two", "three"};
			System.String[] fields = new System.String[]{"f1", "f2", "f3"};
			BooleanClause.Occur[] flags = new BooleanClause.Occur[]{BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT, BooleanClause.Occur.SHOULD};
			Query q = MultiFieldQueryParser.Parse(queries, fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+f1:one -f2:two f3:three", q.ToString());
			
			try
			{
				BooleanClause.Occur[] flags2 = new BooleanClause.Occur[]{BooleanClause.Occur.MUST};
				q = MultiFieldQueryParser.Parse(queries, fields, flags2, new StandardAnalyzer());
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception, array length differs
			}
		}
		
		[Test]
		public virtual void  TestStaticMethod3Old()
		{
			System.String[] queries = new System.String[]{"one", "two"};
			System.String[] fields = new System.String[]{"b", "t"};
			BooleanClause.Occur[] flags = new BooleanClause.Occur[]{BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
			Query q = MultiFieldQueryParser.Parse(queries, fields, flags, new StandardAnalyzer());
			Assert.AreEqual("+b:one -t:two", q.ToString());
			
			try
			{
				BooleanClause.Occur[] flags2 = new BooleanClause.Occur[]{BooleanClause.Occur.MUST};
				q = MultiFieldQueryParser.Parse(queries, fields, flags2, new StandardAnalyzer());
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception, array length differs
			}
		}
		
		[Test]
		public virtual void  TestAnalyzerReturningNull()
		{
			System.String[] fields = new System.String[]{"f1", "f2", "f3"};
			MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new AnalyzerReturningNull());
			Query q = parser.Parse("bla AND blo");
			Assert.AreEqual("+(f2:bla f3:bla) +(f2:blo f3:blo)", q.ToString());
			// the following queries are not affected as their terms are not analyzed anyway:
			q = parser.Parse("bla*");
			Assert.AreEqual("f1:bla* f2:bla* f3:bla*", q.ToString());
			q = parser.Parse("bla~");
			Assert.AreEqual("f1:bla~0.5 f2:bla~0.5 f3:bla~0.5", q.ToString());
			q = parser.Parse("[a TO c]");
			Assert.AreEqual("f1:[a TO c] f2:[a TO c] f3:[a TO c]", q.ToString());
		}
		
		[Test]
		public virtual void  TestStopWordSearching()
		{
			Analyzer analyzer = new StandardAnalyzer();
			Directory ramDir = new RAMDirectory();
			IndexWriter iw = new IndexWriter(ramDir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("body", "blah the footest blah", Field.Store.NO, Field.Index.ANALYZED));
			iw.AddDocument(doc);
			iw.Close();
			
			MultiFieldQueryParser mfqp = new MultiFieldQueryParser(new System.String[]{"body"}, analyzer);
			mfqp.SetDefaultOperator(QueryParser.Operator.AND);
			Query q = mfqp.Parse("the footest");
			IndexSearcher is_Renamed = new IndexSearcher(ramDir);
			ScoreDoc[] hits = is_Renamed.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			is_Renamed.Close();
		}
		
		/// <summary> Return empty tokens for field "f1".</summary>
		private class AnalyzerReturningNull:Analyzer
		{
			internal StandardAnalyzer stdAnalyzer = new StandardAnalyzer();
			
			public AnalyzerReturningNull()
			{
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				if ("f1".Equals(fieldName))
				{
					return new EmptyTokenStream();
				}
				else
				{
					return stdAnalyzer.TokenStream(fieldName, reader);
				}
			}
			
			private class EmptyTokenStream:TokenStream
			{
				public override bool IncrementToken()
				{
					return false;
				}
			}
		}
	}
}