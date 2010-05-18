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
using Lucene.Net.Util;

using NUnit.Framework;

namespace Lucene.Net.Search
{
    /// <summary>
    /// Summary description for TermsFilterTest
    /// </summary>
    [TestFixture]
    public class TermsFilterTest
    {
        public TermsFilterTest() { }


        [Test]
        public void Cachability_Test()
        {
            TermsFilter a = new TermsFilter();
            a.AddTerm(new Term("field1", "a"));
            a.AddTerm(new Term("field1", "b"));

            // original test used placing filters in a HashSet to
            // determine equality, where the FilterManager uses
            // the hash code of the filters as the key, so
            // it makes more sense to just test the equality of the 
            // hash codes.

            TermsFilter b = new TermsFilter();
            b.AddTerm(new Term("field1", "a"));
            b.AddTerm(new Term("field1", "b"));
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode(), "Hashes do not match");

            b.AddTerm(new Term("field1", "a")); //duplicate term
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode(), "Hashes do not match");

            b.AddTerm(new Term("field1", "c"));
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode(), "Hashes match");
        }

        [Test]
        public void MissingTerms_Test()
        {
            string fieldName = "field1";
            RAMDirectory rd = new RAMDirectory();
            IndexWriter w = new IndexWriter(rd, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            for (int i = 0; i < 100; i++)
            {
                Document doc = new Document();
                int term = i * 10; //terms are units of 10;
                doc.Add(new Field(fieldName, "" + term, Field.Store.YES, Field.Index.NOT_ANALYZED));
                w.AddDocument(doc);
            }
            w.Close();
            IndexReader reader = IndexReader.Open(rd, true);

            TermsFilter tf = new TermsFilter();
            tf.AddTerm(new Term(fieldName, "19"));
            OpenBitSet bits = (OpenBitSet)tf.GetDocIdSet(reader);
            Assert.AreEqual(0, bits.Cardinality(), "Must match nothing");

            tf.AddTerm(new Term(fieldName, "20"));
            bits = (OpenBitSet)tf.GetDocIdSet(reader);
            Assert.AreEqual(1, bits.Cardinality(), "Must match 1");

            tf.AddTerm(new Term(fieldName, "10"));
            bits = (OpenBitSet)tf.GetDocIdSet(reader);
            Assert.AreEqual(2, bits.Cardinality(), "Must match 2");

            tf.AddTerm(new Term(fieldName, "00"));
            bits = (OpenBitSet)tf.GetDocIdSet(reader);
            Assert.AreEqual(2, bits.Cardinality(), "Must match 2");
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void ExOnBits()
        {
            TermsFilter a = new TermsFilter();
            a.AddTerm(new Term("field1", "a"));
            a.AddTerm(new Term("field1", "b"));
            BitArray b = a.Bits(null);
        }
    }
}
