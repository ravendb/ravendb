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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;

using NUnit.Framework;

namespace Lucene.Net.Search
{
    [TestFixture]
    public class DuplicateFilterTest : TestCase
    {
        private static String KEY_FIELD = "url";
        private RAMDirectory directory;
        private IndexReader reader;
        TermQuery tq = new TermQuery(new Term("text", "lucene"));
        private IndexSearcher searcher;

        [SetUp]
        public void SetUp()
        {
            directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(), true);

            //Add series of docs with filterable fields : url, text and dates  flags
            AddDoc(writer, "http://lucene.apache.org", "lucene 1.4.3 available", "20040101");
            AddDoc(writer, "http://lucene.apache.org", "New release pending", "20040102");
            AddDoc(writer, "http://lucene.apache.org", "Lucene 1.9 out now", "20050101");
            AddDoc(writer, "http://www.bar.com", "Local man bites dog", "20040101");
            AddDoc(writer, "http://www.bar.com", "Dog bites local man", "20040102");
            AddDoc(writer, "http://www.bar.com", "Dog uses Lucene", "20050101");
            AddDoc(writer, "http://lucene.apache.org", "Lucene 2.0 out", "20050101");
            AddDoc(writer, "http://lucene.apache.org", "Oops. Lucene 2.1 out", "20050102");

            writer.Close();
            reader = IndexReader.Open(directory,true);
            searcher = new IndexSearcher(reader);

        }

        [TearDown]
        public void TearDown()
        {
            reader.Close();
            searcher.Close();
            directory.Close();
        }

        private void AddDoc(IndexWriter writer, String url, String text, String date)
        {
            Document doc = new Document();
            doc.Add(new Field(KEY_FIELD, url, Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new Field("text", text, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("date", date, Field.Store.YES, Field.Index.ANALYZED));
            writer.AddDocument(doc);
        }

        [Test]
        public void TestDefaultFilter()
        {
            DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
            Hashtable results = new Hashtable();
            Hits h = searcher.Search(tq, df);
            for (int i = 0; i < h.Length(); i++)
            {
                Document d = h.Doc(i);
                String url = d.Get(KEY_FIELD);
                Assert.IsFalse(results.Contains(url), "No duplicate urls should be returned");
                results.Add(url,url);
            }
        }

        [Test]
        public void TestNoFilter()
        {
            Hashtable results = new Hashtable();
            Hits h = searcher.Search(tq);
            Assert.IsTrue(h.Length() > 0, "Default searching should have found some matches");
            bool dupsFound = false;
            for (int i = 0; i < h.Length(); i++)
            {
                Document d = h.Doc(i);
                String url = d.Get(KEY_FIELD);
                if (!dupsFound)
                    dupsFound = results.Contains(url);
                results[url]=url;
            }
            Assert.IsTrue(dupsFound, "Default searching should have found duplicate urls");
        }

        [Test]
        public void TestFastFilter()
        {
            DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
            df.SetProcessingMode(DuplicateFilter.PM_FAST_INVALIDATION);
            Hashtable results = new Hashtable();
            Hits h = searcher.Search(tq, df);
            Assert.IsTrue(h.Length() > 0, "Filtered searching should have found some matches");
            for (int i = 0; i < h.Length(); i++)
            {
                Document d = h.Doc(i);
                String url = d.Get(KEY_FIELD);
                Assert.IsFalse(results.Contains(url), "No duplicate urls should be returned");
                results.Add(url,url);
            }
            Assert.AreEqual(2, results.Count, "Two urls found");
        }

        [Test]
        public void TestKeepsLastFilter()
        {
            DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
            df.SetKeepMode(DuplicateFilter.KM_USE_LAST_OCCURRENCE);
            Hits h = searcher.Search(tq, df);
            Assert.IsTrue(h.Length() > 0, "Filtered searching should have found some matches");
            for (int i = 0; i < h.Length(); i++)
            {
                Document d = h.Doc(i);
                String url = d.Get(KEY_FIELD);
                TermDocs td = reader.TermDocs(new Term(KEY_FIELD, url));
                int lastDoc = 0;
                while (td.Next())
                {
                    lastDoc = td.Doc();
                }
                Assert.AreEqual(lastDoc, h.Id((i)), "Duplicate urls should return last doc");
            }
        }

        [Test]
        public void TestKeepsFirstFilter()
        {
            DuplicateFilter df = new DuplicateFilter(KEY_FIELD);
            df.SetKeepMode(DuplicateFilter.KM_USE_FIRST_OCCURRENCE);
            Hits h = searcher.Search(tq, df);
            Assert.IsTrue(h.Length() > 0, "Filtered searching should have found some matches");
            for (int i = 0; i < h.Length(); i++)
            {
                Document d = h.Doc(i);
                String url = d.Get(KEY_FIELD);
                TermDocs td = reader.TermDocs(new Term(KEY_FIELD, url));
                int lastDoc = 0;
                td.Next();
                lastDoc = td.Doc();
                Assert.AreEqual(lastDoc, h.Id((i)), "Duplicate urls should return first doc");
            }
        }
    }
}
