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
    /// Summary description for TermVectorEnumeratorTest
    /// </summary>
    [TestFixture]
    public class TermVectorEnumeratorTest
    {
        public TermVectorEnumeratorTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        private IndexReader reader;

        #region setup/teardown methods

        [SetUp]
        public void MyTestInitialize()
        {
            RAMDirectory rd = new RAMDirectory();
            IndexWriter writer = new IndexWriter(rd, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            for (int i = 0; i < 5; i++)
            {
                Document doc = new Document();
                string data = string.Format("{0} {1}", i, i * 10);
                doc.Add(new Field("test", data, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
                writer.AddDocument(doc);
            }

            // add document that does not have the vectored field
            Document docDiffField = new Document();
            string diffData = string.Format("{0} {1}", 10, 10 * 10);
            docDiffField.Add(new Field("test2", diffData, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
            writer.AddDocument(docDiffField);


            writer.Close();
            this.reader = IndexReader.Open(rd, false);
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
        public void NoPosNoOffsetTest()
        {
            int doc = 0;
            foreach (TermFreqVector vector in new TermVectorEnumerator(this.reader, "test"))
            {
                if (doc == 0)
                {
                    Assert.AreEqual(1, vector.GetTerms().Length);
                    Assert.AreEqual(doc.ToString(), vector.GetTerms()[0]);
                }
                else if (doc == 5)
                {
                    Assert.IsInstanceOf<EmptyVector>(vector);
                }
                else
                {
                    Assert.AreEqual(2, vector.GetTerms().Length);
                    Assert.AreEqual(doc.ToString(), vector.GetTerms()[0]);
                    Assert.AreEqual((doc * 10).ToString(), vector.GetTerms()[1]);
                }
                doc++;
            }
        }

        [Test]
        public void DeletedDocumentTest()
        {
            this.reader.DeleteDocument(2);
            this.reader.Flush();

            int doc = 0;
            foreach (TermFreqVector vector in new TermVectorEnumerator(this.reader, "test"))
            {
                if (doc == 0)
                {
                    Assert.AreEqual(1, vector.GetTerms().Length);
                    Assert.AreEqual(doc.ToString(), vector.GetTerms()[0]);
                }
                else if (doc == 2 || doc == 5)
                {
                    Assert.IsInstanceOf<EmptyVector>(vector);
                }
                else
                {
                    Assert.AreEqual(2, vector.GetTerms().Length);
                    Assert.AreEqual(doc.ToString(), vector.GetTerms()[0]);
                    Assert.AreEqual((doc * 10).ToString(), vector.GetTerms()[1]);
                }
                doc++;
            }
        }
    }
}
