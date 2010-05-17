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
using System.Text;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Test.Index
{
    /// <summary>
    /// Summary description for FieldEnumeratorTest
    /// </summary>
    [TestFixture]
    public class FieldEnumeratorTest
    {
        public FieldEnumeratorTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        private static IndexReader reader;

        #region setup/teardown methods
        
        [TestFixtureSetUp]
        public static void MyClassInitialize()
        {
            RAMDirectory rd = new RAMDirectory();
            IndexWriter writer = new IndexWriter(rd, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
            for (int i = 0; i < 1000; i++)
            {
                Document doc = new Document();
                doc.Add(new Field("string", i.ToString(), Field.Store.YES, Field.Index.ANALYZED));
                doc.Add(new NumericField("int", Field.Store.YES, true).SetIntValue(i));
                doc.Add(new NumericField("long", Field.Store.YES, true).SetLongValue(i));
                doc.Add(new NumericField("double", Field.Store.YES, true).SetDoubleValue(i));
                doc.Add(new NumericField("float", Field.Store.YES, true).SetFloatValue(i));
                writer.AddDocument(doc);
            }
            writer.Close();
            reader = IndexReader.Open(rd, true);
        }

        [TestFixtureTearDown]
        public static void MyClassCleanup()
        {
            if (reader != null)
            {
                reader.Close();
                reader = null;
            }
        }

        #endregion

        [Test]
        public void StringEnumTest()
        {
            using (StringFieldEnumerator sfe = new StringFieldEnumerator(reader, "string", false))
            {
                int value = 0;
                foreach (string s in sfe.Terms)
                {
                    value++;
                }
                Assert.AreEqual(1000, value);
            }

            // now with the documents
            using (StringFieldEnumerator sfe = new StringFieldEnumerator(reader, "string"))
            {
                int value = 0;
                foreach (string s in sfe.Terms)
                {
                    foreach (int doc in sfe.Docs)
                    {
                        string expected = reader.Document(doc).Get("string");
                        Assert.AreEqual(expected, s);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }
        }

        [Test]
        public void IntEnumTest()
        {
            using (IntFieldEnumerator ife = new IntFieldEnumerator(reader, "string", FieldParser.String))
            {
                int value = 0;
                foreach (int i in ife.Terms)
                {
                    foreach (int doc in ife.Docs)
                    {
                        int expected = Int32.Parse(reader.Document(doc).Get("string"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }

            using (IntFieldEnumerator ife = new IntFieldEnumerator(reader, "int", FieldParser.Numeric))
            {
                int value = 0;
                foreach (int i in ife.Terms)
                {
                    foreach (int doc in ife.Docs)
                    {
                        int expected = Int32.Parse(reader.Document(doc).Get("int"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }
        }

        [Test]
        public void LongEnumTest()
        {
            using (LongFieldEnumerator lfe = new LongFieldEnumerator(reader, "string", FieldParser.String))
            {
                int value = 0;
                foreach (long i in lfe.Terms)
                {
                    foreach (int doc in lfe.Docs)
                    {
                        long expected = Int64.Parse(reader.Document(doc).Get("string"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }

            using (LongFieldEnumerator lfe = new LongFieldEnumerator(reader, "long", FieldParser.Numeric))
            {
                int value = 0;
                foreach (int i in lfe.Terms)
                {
                    foreach (int doc in lfe.Docs)
                    {
                        long expected = Int64.Parse(reader.Document(doc).Get("long"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }
        }

        [Test]
        public void FloatEnumTest()
        {
            using (FloatFieldEnumerator ffe = new FloatFieldEnumerator(reader, "string", FieldParser.String))
            {
                int value = 0;
                foreach (int i in ffe.Terms)
                {
                    foreach (int doc in ffe.Docs)
                    {
                        float expected = Single.Parse(reader.Document(doc).Get("string"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }

            using (FloatFieldEnumerator ffe = new FloatFieldEnumerator(reader, "float", FieldParser.Numeric))
            {
                int value = 0;
                foreach (int i in ffe.Terms)
                {
                    foreach (int doc in ffe.Docs)
                    {
                        float expected = Single.Parse(reader.Document(doc).Get("float"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }
        }

        [Test]
        public void DoubleEnumTest()
        {
            using (DoubleFieldEnumerator dfe = new DoubleFieldEnumerator(reader, "string", FieldParser.String))
            {
                int value = 0;
                foreach (int i in dfe.Terms)
                {
                    foreach (int doc in dfe.Docs)
                    {
                        double expected = Double.Parse(reader.Document(doc).Get("string"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }

            using (DoubleFieldEnumerator dfe = new DoubleFieldEnumerator(reader, "double", FieldParser.Numeric))
            {
                int value = 0;
                foreach (int i in dfe.Terms)
                {
                    foreach (int doc in dfe.Docs)
                    {
                        double expected = Double.Parse(reader.Document(doc).Get("double"));
                        Assert.AreEqual(expected, i);
                    }
                    value++;
                }
                Assert.AreEqual(1000, value);
            }
        }

        [Test]
        public void TermDocEnumeratorOnlyTestSingleTerm()
        {
            Term t = new Term("string", "500");
            using (TermDocEnumerator tde = new TermDocEnumerator(reader.TermDocs()))
            {
                tde.Seek(t);
                int count = 0;
                foreach (int doc in tde)
                {
                    Assert.AreEqual(500, doc);
                    count++;
                }
                Assert.AreEqual(1, count);
            }
        }

        [Test]
        public void TermDocEnumeratorOnlyTestMultipleTerms()
        {
            HashSet<Term> terms = new HashSet<Term>();
            terms.Add(new Term("string", "500"));
            terms.Add(new Term("string", "600"));
            terms.Add(new Term("string", "400"));

            HashSet<int> docs = new HashSet<int>();
            using (TermDocEnumerator tde = new TermDocEnumerator(reader.TermDocs()))
            {
                foreach (Term t in terms)
                {
                    tde.Seek(t);
                    foreach (int doc in tde)
                    {
                        docs.Add(doc);
                    }
                }
            }

            Assert.AreEqual(3, docs.Count);
            Assert.IsTrue(docs.Contains(400));
            Assert.IsTrue(docs.Contains(500));
            Assert.IsTrue(docs.Contains(600));
        }
    }
}
