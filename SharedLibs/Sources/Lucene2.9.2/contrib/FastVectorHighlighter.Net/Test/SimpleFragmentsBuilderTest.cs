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
using System.Collections.Generic;
using System.Text;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Search;
using Lucene.Net.Documents;
using Lucene.Net.QueryParsers;
using Lucene.Net.Store;
using Lucene.Net.Index;
using Lucene.Net.Util;

using NUnit.Framework;

namespace Lucene.Net.Search.Vectorhighlight
{
    [TestFixture]
    public class SimpleFragmentsBuilderTest : AbstractTestCase
    {
        [Test]
        public void Test1TermIndex()
        {
            FieldFragList ffl = this.ffl("a", "a");
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.AreEqual("<b>a</b>", sfb.CreateFragment(reader, 0, F, ffl));

            // change tags
            sfb = new SimpleFragmentsBuilder(new String[] { "[" }, new String[] { "]" });
            Assert.AreEqual("[a]", sfb.CreateFragment(reader, 0, F, ffl));
        }

        [Test]
        public void Test2Frags()
        {
            FieldFragList ffl = this.ffl("a", "a b b b b b b b b b b b a b a b");
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            String[] f = sfb.CreateFragments(reader, 0, F, ffl, 3);
            // 3 snippets requested, but should be 2
            Assert.AreEqual(2, f.Length);
            Assert.AreEqual("<b>a</b> b b b b b b b b b ", f[0]);
            Assert.AreEqual("b b <b>a</b> b <b>a</b> b", f[1]);
        }

        [Test]
        public void Test3Frags()
        {
            FieldFragList ffl = this.ffl("a c", "a b b b b b b b b b b b a b a b b b b b c a a b b");
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            String[] f = sfb.CreateFragments(reader, 0, F, ffl, 3);
            Assert.AreEqual(3, f.Length);
            Assert.AreEqual("<b>a</b> b b b b b b b b b ", f[0]);
            Assert.AreEqual("b b <b>a</b> b <b>a</b> b b b b b ", f[1]);
            Assert.AreEqual("<b>c</b> <b>a</b> <b>a</b> b b", f[2]);
        }


        private FieldFragList ffl(String queryValue, String indexValue)
        {
            Make1d1fIndex(indexValue);
            Query query = paW.Parse(queryValue);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            return new SimpleFragListBuilder().CreateFieldFragList(fpl, 20);
        }

        [Test]
        public void Test1PhraseShortMV()
        {
            MakeIndexShortMV();

            FieldQuery fq = new FieldQuery(Tq("d"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl, 100);
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.AreEqual("a b c <b>d</b> e", sfb.CreateFragment(reader, 0, F, ffl));
        }

        [Test]
        public void Test1PhraseLongMV()
        {
            MakeIndexLongMV();

            FieldQuery fq = new FieldQuery(PqF("search", "engines"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl, 100);
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.AreEqual(" most <b>search engines</b> use only one of these methods. Even the <b>search engines</b> that says they can use t",
                sfb.CreateFragment(reader, 0, F, ffl));
        }

        [Test]
        public void Test1PhraseLongMVB()
        {
            MakeIndexLongMVB();

            FieldQuery fq = new FieldQuery(PqF("sp", "pe", "ee", "ed"), true, true); // "speed" -(2gram)-> "sp","pe","ee","ed"
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl, 100);
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.AreEqual("ssing <b>speed</b>, the", sfb.CreateFragment(reader, 0, F, ffl));
        }

        [Test]
        public void TestUnstoredField()
        {
            MakeUnstoredIndex();

            FieldQuery fq = new FieldQuery(Tq("aaa"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl, 100);
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.IsNull(sfb.CreateFragment(reader, 0, F, ffl));
        }

        protected void MakeUnstoredIndex()
        {
            IndexWriter writer = new IndexWriter(dir, analyzerW, true, IndexWriter.MaxFieldLength.LIMITED);
            Document doc = new Document();
            doc.Add(new Field(F, "aaa", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
            writer.AddDocument(doc);
            writer.Close();

            reader = IndexReader.Open(dir, true);
        }

        [Test]
        public void Test1StrMV()
        {
            MakeIndexStrMV();

            FieldQuery fq = new FieldQuery(Tq("defg"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl, 100);
            SimpleFragmentsBuilder sfb = new SimpleFragmentsBuilder();
            Assert.AreEqual("abc<b>defg</b>hijkl", sfb.CreateFragment(reader, 0, F, ffl));
        }
    }
}
