/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Documents;
using Lucene.Net.Store;
using Lucene.Net.Search;
using Lucene.Net.Analysis;
using Lucene.Net.Index;

using NUnit.Framework;

namespace Lucene.Net.Search
{
    [TestFixture]
    public class FuzzyLikeThisQueryTest : Lucene.Net.TestCase
    {
        private RAMDirectory directory;
        private IndexSearcher searcher;
        private Analyzer analyzer = new WhitespaceAnalyzer();

        [SetUp]
        public void SetUp()
        {
            directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);

            //Add series of docs with misspelt names
            AddDoc(writer, "jonathon smythe", "1");
            AddDoc(writer, "jonathan smith", "2");
            AddDoc(writer, "johnathon smyth", "3");
            AddDoc(writer, "johnny smith", "4");
            AddDoc(writer, "jonny smith", "5");
            AddDoc(writer, "johnathon smythe", "6");

            writer.Close();
            searcher = new IndexSearcher(directory,true);
        }

        private void AddDoc(IndexWriter writer, String name, String id)
        {
            Document doc = new Document();
            doc.Add(new Field("name", name, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("id", id, Field.Store.YES, Field.Index.ANALYZED));
            writer.AddDocument(doc);
        }


        //Tests that idf ranking is not favouring rare mis-spellings over a strong edit-distance match 
        [Test]
        public void TestClosestEditDistanceMatchComesFirst()
        {
            FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
            flt.AddTerms("smith", "name", 0.3f, 1);
            Query q = flt.Rewrite(searcher.GetIndexReader());
            Hashtable queryTerms = new Hashtable();
            q.ExtractTerms(queryTerms);
            Assert.IsTrue(queryTerms.Contains(new Term("name", "smythe")),"Should have variant smythe");
            Assert.IsTrue(queryTerms.Contains(new Term("name", "smith")), "Should have variant smith");
            Assert.IsTrue(queryTerms.Contains(new Term("name", "smyth")), "Should have variant smyth");
            TopDocs topDocs = searcher.Search(flt, 1);
            ScoreDoc[] sd = topDocs.scoreDocs;
            Assert.IsTrue((sd != null) && (sd.Length > 0), "score docs must match 1 doc");
            Document doc = searcher.Doc(sd[0].doc);
            Assert.AreEqual("2", doc.Get("id"), "Should match most similar not most rare variant");
        }

        //Test multiple input words are having variants produced
        [Test]
        public void TestMultiWord()
        {
            FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
            flt.AddTerms("jonathin smoth", "name", 0.3f, 1);
            Query q = flt.Rewrite(searcher.GetIndexReader());
            Hashtable queryTerms = new Hashtable();
            q.ExtractTerms(queryTerms);
            Assert.IsTrue(queryTerms.Contains(new Term("name", "jonathan")),"Should have variant jonathan");
            Assert.IsTrue(queryTerms.Contains(new Term("name", "smith")), "Should have variant smith");
            TopDocs topDocs = searcher.Search(flt, 1);
            ScoreDoc[] sd = topDocs.scoreDocs;
            Assert.IsTrue((sd != null) && (sd.Length > 0), "score docs must match 1 doc");
            Document doc = searcher.Doc(sd[0].doc);
            Assert.AreEqual("2", doc.Get("id"), "Should match most similar when using 2 words");
        }

        //Test bug found when first query word does not match anything
        [Test]
        public void TestNoMatchFirstWordBug()
        {
            FuzzyLikeThisQuery flt = new FuzzyLikeThisQuery(10, analyzer);
            flt.AddTerms("fernando smith", "name", 0.3f, 1);
            Query q = flt.Rewrite(searcher.GetIndexReader());
            Hashtable queryTerms = new Hashtable();
            q.ExtractTerms(queryTerms);
            Assert.IsTrue(queryTerms.Contains(new Term("name", "smith")), "Should have variant smith");
            TopDocs topDocs = searcher.Search(flt, 1);
            ScoreDoc[] sd = topDocs.scoreDocs;
            Assert.IsTrue((sd != null) && (sd.Length > 0), "score docs must match 1 doc");
            Document doc = searcher.Doc(sd[0].doc);
            Assert.AreEqual("2", doc.Get("id"), "Should match most similar when using 2 words");
        }

        [Test]
        public void TestFuzzyLikeThisQueryEquals()
        {
            Analyzer analyzer = new WhitespaceAnalyzer();
            FuzzyLikeThisQuery fltq1 = new FuzzyLikeThisQuery(10, analyzer);
            fltq1.AddTerms("javi", "subject", 0.5f, 2);
            FuzzyLikeThisQuery fltq2 = new FuzzyLikeThisQuery(10, analyzer);
            fltq2.AddTerms("javi", "subject", 0.5f, 2);
            Assert.AreEqual(fltq1, fltq2, "FuzzyLikeThisQuery with same attributes is not equal");
        }
    }
}
