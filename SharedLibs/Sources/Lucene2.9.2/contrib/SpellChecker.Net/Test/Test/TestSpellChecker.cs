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

using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Document = Lucene.Net.Documents.Document;
using English = Lucene.Net.Test.Util.Spell.English;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using Directory = Lucene.Net.Store.Directory;
using LuceneDictionary = SpellChecker.Net.Search.Spell.LuceneDictionary;

namespace SpellChecker.Net.Test.Search.Spell
{
	
	
	/// <summary> Test case
	/// 
	/// </summary>
	/// <author>  Nicolas Maisonneuve
	/// </author>
	[TestFixture]
    public class TestSpellChecker
	{
		private SpellChecker.Net.Search.Spell.SpellChecker spellChecker;
		private Directory userindex, spellindex;
		
        [SetUp]
        public virtual void  SetUp()
		{
			//create a user index
			userindex = new RAMDirectory();
			IndexWriter writer = new IndexWriter(userindex, new SimpleAnalyzer(), true);
			
			for (int i = 0; i < 1000; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("field1", English.IntToEnglish(i), Field.Store.YES, Field.Index.TOKENIZED));
				doc.Add(new Field("field2", English.IntToEnglish(i + 1), Field.Store.YES, Field.Index.TOKENIZED)); // + word thousand
				writer.AddDocument(doc);
			}
			writer.Close();
			
			// create the spellChecker
			spellindex = new RAMDirectory();
			spellChecker = new SpellChecker.Net.Search.Spell.SpellChecker(spellindex);
		}
		
		[Test]
		public virtual void  TestBuild()
		{
			try
			{
				IndexReader r = IndexReader.Open(userindex);
				
				spellChecker.ClearIndex();
				
				Addwords(r, "field1");
				int num_field1 = this.Numdoc();
				
				Addwords(r, "field2");
				int num_field2 = this.Numdoc();
				
				Assert.AreEqual(num_field2, num_field1 + 1);
				
				// test small word
				System.String[] similar = spellChecker.SuggestSimilar("fvie", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "five");
				
				similar = spellChecker.SuggestSimilar("five", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "nine"); // don't suggest a word for itself
				
				similar = spellChecker.SuggestSimilar("fiv", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "five");
				
				similar = spellChecker.SuggestSimilar("ive", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "five");
				
				similar = spellChecker.SuggestSimilar("fives", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "five");
				
				similar = spellChecker.SuggestSimilar("fie", 2);
				Assert.AreEqual(1, similar.Length);
				Assert.AreEqual(similar[0], "five");
				
				similar = spellChecker.SuggestSimilar("fi", 2);
				Assert.AreEqual(0, similar.Length);
				
				// test restraint to a field
				similar = spellChecker.SuggestSimilar("tousand", 10, r, "field1", false);
				Assert.AreEqual(0, similar.Length); // there isn't the term thousand in the field field1
				
				similar = spellChecker.SuggestSimilar("tousand", 10, r, "field2", false);
				Assert.AreEqual(1, similar.Length); // there is the term thousand in the field field2
			}
			catch (System.IO.IOException e)
			{
                System.Console.Error.WriteLine(e.StackTrace);
				Assert.Fail();
			}
		}
		
		private void  Addwords(IndexReader r, System.String field)
		{
			long time = (System.DateTime.Now.Ticks - 621355968000000000) / 10000;
			spellChecker.IndexDictionary(new LuceneDictionary(r, field));
			time = (System.DateTime.Now.Ticks - 621355968000000000) / 10000 - time;
			//System.out.println("time to build " + field + ": " + time);
		}
		
		private int Numdoc()
		{
			IndexReader rs = IndexReader.Open(spellindex);
			int num = rs.NumDocs();
			Assert.IsTrue(num != 0);
			//System.out.println("num docs: " + num);
			rs.Close();
			return num;
		}
	}
}
