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

using Lucene.Net.Documents;
using Lucene.Net.Search;

using NUnit.Framework;

namespace Lucene.Net.Search.Vectorhighlight
{
    [TestFixture]
    public class FieldPhraseListTest : AbstractTestCase
    {
        [Test]
        public void Test1TermIndex()
        {
            Make1d1fIndex("a");

            FieldQuery fq = new FieldQuery(Tq("a"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("a(1.0)((0,1))", fpl.phraseList.First.Value.ToString());

            fq = new FieldQuery(Tq("b"), true, true);
            stack = new FieldTermStack(reader, 0, F, fq);
            fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(0, fpl.phraseList.Count);
        }

        [Test]
        public void Test2TermsIndex()
        {
            Make1d1fIndex("a a");

            FieldQuery fq = new FieldQuery(Tq("a"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(2, fpl.phraseList.Count);
            Assert.AreEqual("a(1.0)((0,1))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual("a(1.0)((2,3))", fpl.phraseList.First.Next.Value.ToString());
        }

        [Test]
        public void Test1PhraseIndex()
        {
            Make1d1fIndex("a b");

            FieldQuery fq = new FieldQuery(PqF("a", "b"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("ab(1.0)((0,3))", fpl.phraseList.First.Value.ToString());

            fq = new FieldQuery(Tq("b"), true, true);
            stack = new FieldTermStack(reader, 0, F, fq);
            fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("b(1.0)((2,3))", fpl.phraseList.First.Value.ToString());
        }

        [Test]
        public void Test1PhraseIndexB()
        {
            // 01 12 23 34 45 56 67 78 (offsets)
            // bb|bb|ba|ac|cb|ba|ab|bc
            //  0  1  2  3  4  5  6  7 (positions)
            Make1d1fIndexB("bbbacbabc");

            FieldQuery fq = new FieldQuery(PqF("ba", "ac"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("baac(1.0)((2,5))", fpl.phraseList.First.Value.ToString());
        }

        [Test]
        public void Test2ConcatTermsIndexB()
        {
            // 01 12 23 (offsets)
            // ab|ba|ab
            //  0  1  2 (positions)
            Make1d1fIndexB("abab");

            FieldQuery fq = new FieldQuery(Tq("ab"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(2, fpl.phraseList.Count);
            Assert.AreEqual("ab(1.0)((0,2))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual("ab(1.0)((2,4))", fpl.phraseList.First.Next.Value.ToString());
        }

        [Test]
        public void Test2Terms1PhraseIndex()
        {
            Make1d1fIndex("c a a b");

            // phraseHighlight = true
            FieldQuery fq = new FieldQuery(PqF("a", "b"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("ab(1.0)((4,7))", fpl.phraseList.First.Value.ToString());

            // phraseHighlight = false
            fq = new FieldQuery(PqF("a", "b"), false, true);
            stack = new FieldTermStack(reader, 0, F, fq);
            fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(2, fpl.phraseList.Count);
            Assert.AreEqual("a(1.0)((2,3))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual("ab(1.0)((4,7))", fpl.phraseList.First.Next.Value.ToString());
        }

        [Test]
        public void TestPhraseSlop()
        {
            Make1d1fIndex("c a a b c");

            FieldQuery fq = new FieldQuery(pqF(2F, 1, "a", "c"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("ac(2.0)((4,5)(8,9))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(4, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(9, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void Test2PhrasesOverlap()
        {
            Make1d1fIndex("d a b c d");

            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "b"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            query.Add(PqF("b", "c"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("abc(1.0)((2,7))", fpl.phraseList.First.Value.ToString());
        }

        [Test]
        public void Test3TermsPhrase()
        {
            Make1d1fIndex("d a b a b c d");

            FieldQuery fq = new FieldQuery(PqF("a", "b", "c"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("abc(1.0)((6,11))", fpl.phraseList.First.Value.ToString());
        }

        [Test]
        public void TestSearchLongestPhrase()
        {
            Make1d1fIndex("d a b d c a b c");

            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "b"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            query.Add(PqF("a", "b", "c"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(2, fpl.phraseList.Count);
            Assert.AreEqual("ab(1.0)((2,5))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual("abc(1.0)((10,15))", fpl.phraseList.First.Next.Value.ToString());
        }

        [Test]
        public void Test1PhraseShortMV()
        {
            MakeIndexShortMV();

            FieldQuery fq = new FieldQuery(Tq("d"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("d(1.0)((6,7))", fpl.phraseList.First.Value.ToString());
        }

        [Test]
        public void Test1PhraseLongMV()
        {
            MakeIndexLongMV();

            FieldQuery fq = new FieldQuery(PqF("search", "engines"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(2, fpl.phraseList.Count);
            Assert.AreEqual("searchengines(1.0)((102,116))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual("searchengines(1.0)((157,171))", fpl.phraseList.First.Next.Value.ToString());
        }

        [Test]
        public void Test1PhraseLongMVB()
        {
            MakeIndexLongMVB();

            FieldQuery fq = new FieldQuery(PqF("sp", "pe", "ee", "ed"), true, true); // "speed" -(2gram)-> "sp","pe","ee","ed"
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("sppeeeed(1.0)((88,93))", fpl.phraseList.First.Value.ToString());
        }
    }
}
