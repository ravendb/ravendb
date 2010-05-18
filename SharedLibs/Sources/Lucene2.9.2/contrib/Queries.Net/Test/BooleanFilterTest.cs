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

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

using NUnit.Framework;

namespace Lucene.Net.Search
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestFixture]
    public class BooleanFilterTest
    {
        public BooleanFilterTest() { }

        private IndexReader reader;

        #region setup/teardown methods

        [SetUp]
        public void MyTestInitialize()
        {
            RAMDirectory directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

            //Add series of docs with filterable fields : acces rights, prices, dates and "in-stock" flags
            AddDoc(writer, "admin guest", "010", "20040101", "Y");
            AddDoc(writer, "guest", "020", "20040101", "Y");
            AddDoc(writer, "guest", "020", "20050101", "Y");
            AddDoc(writer, "admin", "020", "20050101", "Maybe");
            AddDoc(writer, "admin guest", "030", "20050101", "N");

            writer.Close();
            reader = IndexReader.Open(directory, true);
        }
        

         [TearDown]
         public void MyTestCleanup() 
         {
             if (this.reader != null)
             {
                 this.reader.Close();
                 this.reader = null;
             }
         }
        
        #endregion

        [Test]
        public void Should_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("price", "030", old), BooleanClause.Occur.SHOULD));
                TstFilterCard("Should retrieves only 1 doc", 1, booleanFilter);
            }
        }

        [Test]
        public void Shoulds_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "010", "020", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "020", "030", old), BooleanClause.Occur.SHOULD));
                TstFilterCard("Shoulds are Ored together", 5, booleanFilter);
            }
        }

        [Test]
        public void ShouldsAndMustNot_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "010", "020", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "020", "030", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", old), BooleanClause.Occur.MUST_NOT));
                TstFilterCard("Shoulds Ored but AndNot", 4, booleanFilter);

                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("inStock", "Maybe", old), BooleanClause.Occur.MUST_NOT));
                TstFilterCard("Shoulds Ored but AndNots", 3, booleanFilter);
            }

        }

        [Test]
        public void ShouldsAndMust_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "010", "020", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "020", "030", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("accessRights", "admin", old), BooleanClause.Occur.MUST));
                TstFilterCard("Shoulds Ored but MUST", 3, booleanFilter);
            }
        }

        [Test]
        public void ShouldsAndMusts_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "010", "020", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "020", "030", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("accessRights", "admin", old), BooleanClause.Occur.MUST));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("date", "20040101", "20041231", old), BooleanClause.Occur.MUST));
                TstFilterCard("Shoulds Ored but MUSTs ANDED", 1, booleanFilter);
            }
        }

        [Test]
        public void ShouldsAndMustsAndMustNot_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("price", "030", "040", old), BooleanClause.Occur.SHOULD));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("accessRights", "admin", old), BooleanClause.Occur.MUST));
                booleanFilter.Add(new BooleanFilterClause(GetRangeFilter("date", "20050101", "20051231", old), BooleanClause.Occur.MUST));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", old), BooleanClause.Occur.MUST_NOT));
                TstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot", 0, booleanFilter);
            }
        }

        [Test]
        public void JustMust_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("accessRights", "admin", old), BooleanClause.Occur.MUST));
                TstFilterCard("MUST", 3, booleanFilter);
            }
        }

        [Test]
        public void JustMustNot_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", old), BooleanClause.Occur.MUST_NOT));
                TstFilterCard("MUST_NOT", 4, booleanFilter);
            }
        }

        [Test]
        public void MustAndMustNot_Test()
        {
            for (int i = 0; i < 2; i++)
            {
                bool old = (i == 0);

                BooleanFilter booleanFilter = new BooleanFilter();
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", old), BooleanClause.Occur.MUST));
                booleanFilter.Add(new BooleanFilterClause(GetTermsFilter("price", "030", old), BooleanClause.Occur.MUST_NOT));
                TstFilterCard("MUST_NOT wins over MUST for same docs", 0, booleanFilter);
            }
        }

        [Test]
        public void HashEquality()
        {
            BooleanFilter a = new BooleanFilter();
            a.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", false), BooleanClause.Occur.MUST));
            a.Add(new BooleanFilterClause(GetTermsFilter("price", "030", false), BooleanClause.Occur.MUST_NOT));

            BooleanFilter b = new BooleanFilter();
            b.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", false), BooleanClause.Occur.MUST));
            b.Add(new BooleanFilterClause(GetTermsFilter("price", "030", false), BooleanClause.Occur.MUST_NOT));

            Assert.AreEqual(a.GetHashCode(), b.GetHashCode(), "Hashes do not match");
        }

        [Test]
        public void ToString_Test()
        {
            BooleanFilter b = new BooleanFilter();
            b.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", false), BooleanClause.Occur.MUST));
            b.Add(new BooleanFilterClause(GetTermsFilter("price", "030", false), BooleanClause.Occur.MUST_NOT));
            b.Add(new BooleanFilterClause(GetRangeFilter("price", "030", "040", false), BooleanClause.Occur.SHOULD));

            Assert.AreEqual("BooleanFilter( price:[030 TO 040] +( inStock:N ) -( price:030 ))", b.ToString());
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void ExOnBits_Test()
        {
            BooleanFilter b = new BooleanFilter();
            b.Add(new BooleanFilterClause(GetTermsFilter("inStock", "N", false), BooleanClause.Occur.MUST));
            b.Add(new BooleanFilterClause(GetTermsFilter("price", "030", false), BooleanClause.Occur.MUST_NOT));
            b.Add(new BooleanFilterClause(GetRangeFilter("price", "030", "040", false), BooleanClause.Occur.SHOULD));

            BitArray bits = b.Bits(this.reader);
        }

        #region helpers

        private void AddDoc(IndexWriter writer, String accessRights, String price, String date, String inStock)
        {
            Document doc = new Document();
            doc.Add(new Field("accessRights", accessRights, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("price", price, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("date", date, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("inStock", inStock, Field.Store.YES, Field.Index.ANALYZED));
            writer.AddDocument(doc);
        }

        private Filter GetOldBitSetFilter(Filter filter)
        {
            return new MockBooleanFilter(filter);
        }

        private Filter GetRangeFilter(String field, String lowerPrice, String upperPrice, bool old)
        {
            Filter f = new TermRangeFilter(field, lowerPrice, upperPrice, true, true);
            if (old)
            {
                return GetOldBitSetFilter(f);
            }

            return f;
        }

        private Filter GetTermsFilter(String field, String text, bool old)
        {
            TermsFilter tf = new TermsFilter();
            tf.AddTerm(new Term(field, text));
            if (old)
            {
                return GetOldBitSetFilter(tf);
            }

            return tf;
        }

        private void TstFilterCard(String mes, int expected, Filter filt)
        {
            DocIdSetIterator disi = filt.GetDocIdSet(reader).Iterator();
            int actual = 0;
            while (disi.NextDoc() != DocIdSetIterator.NO_MORE_DOCS)
            {
                actual++;
            }
            Assert.AreEqual(expected, actual, mes);
        }

        #endregion
    }

    public class MockBooleanFilter : Filter
    {
        private Filter filter;

        public MockBooleanFilter(Filter f)
        {
            this.filter = f;
        }

        [Obsolete]
        public override BitArray Bits(IndexReader reader)
        {
            BitArray bits = new BitArray(reader.MaxDoc());
            DocIdSetIterator it = filter.GetDocIdSet(reader).Iterator();
            int doc = DocIdSetIterator.NO_MORE_DOCS;
            while ((doc = it.NextDoc()) != DocIdSetIterator.NO_MORE_DOCS)
            {
                bits.Set(doc, true);
            }
            return bits;
        }
    }
}
