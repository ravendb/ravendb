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
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Store;
using Lucene.Net.Util;


using NUnit.Framework;

namespace Lucene.Net.Search.Similar
{
    [TestFixture]
    public class TestMoreLikeThis : LuceneTestCase
    {
        private RAMDirectory directory;
        private IndexReader reader;
        private IndexSearcher searcher;

        [SetUp]
        public new void SetUp()
        {
            base.SetUp();
            directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29),true, IndexWriter.MaxFieldLength.UNLIMITED);

            // Add series of docs with specific information for MoreLikeThis
            AddDoc(writer, "lucene");
            AddDoc(writer, "lucene release");

            writer.Close();
            reader = IndexReader.Open(directory, true);
            searcher = new IndexSearcher(reader);

        }

        [TearDown]
        public new void TearDown()
        {
            reader.Close();
            searcher.Close();
            directory.Close();
            base.TearDown();
        }

        private void AddDoc(IndexWriter writer, String text)
        {
            Document doc = new Document();
            doc.Add(new Field("text", text, Field.Store.YES, Field.Index.ANALYZED));
            writer.AddDocument(doc);
        }

        [Test]
        public void TestBoostFactor()
        {
            Hashtable originalValues = GetOriginalValues();

            MoreLikeThis mlt = new MoreLikeThis(
                reader);
            mlt.SetMinDocFreq(1);
            mlt.SetMinTermFreq(1);
            mlt.SetMinWordLen(1);
            mlt.SetFieldNames(new String[] { "text" });
            mlt.SetBoost(true);

            // this mean that every term boost factor will be multiplied by this
            // number
            float boostFactor = 5;
            mlt.SetBoostFactor(boostFactor);

            BooleanQuery query = (BooleanQuery)mlt.Like(new System.IO.StringReader("lucene release"));
            IList clauses = query.Clauses();

            Assert.AreEqual(originalValues.Count, clauses.Count,"Expected " + originalValues.Count + " clauses.");

            for (int i = 0; i < clauses.Count; i++)
            {
                BooleanClause clause = (BooleanClause)clauses[i];
                TermQuery tq = (TermQuery)clause.GetQuery();
                float termBoost = (float)originalValues[tq.GetTerm().Text()];
                Assert.IsNotNull(termBoost,"Expected term " + tq.GetTerm().Text());

                float totalBoost = termBoost * boostFactor;
                Assert.AreEqual(totalBoost, tq.GetBoost(), 0.0001,"Expected boost of " + totalBoost + " for term '"
                                 + tq.GetTerm().Text() + "' got " + tq.GetBoost());
            }
        }

        private Hashtable GetOriginalValues()
        {
            Hashtable originalValues = new Hashtable();
            MoreLikeThis mlt = new MoreLikeThis(reader);
            mlt.SetMinDocFreq(1);
            mlt.SetMinTermFreq(1);
            mlt.SetMinWordLen(1);
            mlt.SetFieldNames(new String[] { "text" });
            mlt.SetBoost(true);
            BooleanQuery query = (BooleanQuery)mlt.Like(new System.IO.StringReader("lucene release"));
            IList clauses = query.Clauses();

            for (int i = 0; i < clauses.Count; i++)
            {
                BooleanClause clause = (BooleanClause)clauses[i];
                TermQuery tq = (TermQuery)clause.GetQuery();
                originalValues.Add(tq.GetTerm().Text(), tq.GetBoost());
            }
            return originalValues;
        }
    }
}
