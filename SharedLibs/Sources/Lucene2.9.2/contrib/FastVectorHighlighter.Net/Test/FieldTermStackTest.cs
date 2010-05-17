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
    public class FieldTermStackTest : AbstractTestCase
    {
        [Test]
        public void Test1Term()
        {
            MakeIndex();

            FieldQuery fq = new FieldQuery(Tq("a"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(6, stack.termList.Count);
            Assert.AreEqual("a(0,1,0)", stack.Pop().ToString());
            Assert.AreEqual("a(2,3,1)", stack.Pop().ToString());
            Assert.AreEqual("a(4,5,2)", stack.Pop().ToString());
            Assert.AreEqual("a(12,13,6)", stack.Pop().ToString());
            Assert.AreEqual("a(28,29,14)", stack.Pop().ToString());
            Assert.AreEqual("a(32,33,16)", stack.Pop().ToString());
        }

        [Test]
        public void Test2Terms()
        {
            MakeIndex();

            BooleanQuery query = new BooleanQuery();
            query.Add(Tq("b"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            query.Add(Tq("c"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(8, stack.termList.Count);
            Assert.AreEqual("b(6,7,3)", stack.Pop().ToString());
            Assert.AreEqual("b(8,9,4)", stack.Pop().ToString());
            Assert.AreEqual("c(10,11,5)", stack.Pop().ToString());
            Assert.AreEqual("b(14,15,7)", stack.Pop().ToString());
            Assert.AreEqual("b(16,17,8)", stack.Pop().ToString());
            Assert.AreEqual("c(18,19,9)", stack.Pop().ToString());
            Assert.AreEqual("b(26,27,13)", stack.Pop().ToString());
            Assert.AreEqual("b(30,31,15)", stack.Pop().ToString());
        }

        [Test]
        public void Test1Phrase()
        {
            MakeIndex();

            FieldQuery fq = new FieldQuery(PqF("c", "d"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(3, stack.termList.Count);
            Assert.AreEqual("c(10,11,5)", stack.Pop().ToString());
            Assert.AreEqual("c(18,19,9)", stack.Pop().ToString());
            Assert.AreEqual("d(20,21,10)", stack.Pop().ToString());
        }

        private void MakeIndex()
        {
            //           111111111122222
            // 0123456789012345678901234 (offsets)
            // a a a b b c a b b c d e f
            // 0 1 2 3 4 5 6 7 8 9101112 (position)
            String value1 = "a a a b b c a b b c d e f";
            // 222233333
            // 678901234 (offsets)
            // b a b a f
            //1314151617 (position)
            String value2 = "b a b a f";

            Make1dmfIndex(value1, value2);
        }

        [Test]
        public void Test1TermB()
        {
            makeIndexB();

            FieldQuery fq = new FieldQuery(Tq("ab"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(2, stack.termList.Count);
            Assert.AreEqual("ab(2,4,2)", stack.Pop().ToString());
            Assert.AreEqual("ab(6,8,6)", stack.Pop().ToString());
        }

        [Test]
        public void Test2TermsB()
        {
            makeIndexB();

            BooleanQuery query = new BooleanQuery();
            query.Add(Tq("bc"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            query.Add(Tq("ef"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(3, stack.termList.Count);
            Assert.AreEqual("bc(4,6,4)", stack.Pop().ToString());
            Assert.AreEqual("bc(8,10,8)", stack.Pop().ToString());
            Assert.AreEqual("ef(11,13,11)", stack.Pop().ToString());
        }

        [Test]
        public void Test1PhraseB()
        {
            makeIndexB();

            FieldQuery fq = new FieldQuery(PqF("ab", "bb"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(4, stack.termList.Count);
            Assert.AreEqual("ab(2,4,2)", stack.Pop().ToString());
            Assert.AreEqual("bb(3,5,3)", stack.Pop().ToString());
            Assert.AreEqual("ab(6,8,6)", stack.Pop().ToString());
            Assert.AreEqual("bb(7,9,7)", stack.Pop().ToString());
        }
                
        private void makeIndexB()
        {
            //                             1 11 11
            // 01 12 23 34 45 56 67 78 89 90 01 12 (offsets)
            // aa|aa|ab|bb|bc|ca|ab|bb|bc|cd|de|ef
            //  0  1  2  3  4  5  6  7  8  9 10 11 (position)
            String value = "aaabbcabbcdef";

            Make1dmfIndexB(value);
        }

        [Test]
        public void Test1PhraseShortMV()
        {
            MakeIndexShortMV();

            FieldQuery fq = new FieldQuery(Tq("d"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("d(6,7,3)", stack.Pop().ToString());
        }

        [Test]
        public void Test1PhraseLongMV()
        {
            MakeIndexLongMV();

            FieldQuery fq = new FieldQuery(PqF("search", "engines"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(4, stack.termList.Count);
            Assert.AreEqual("search(102,108,14)", stack.Pop().ToString());
            Assert.AreEqual("engines(109,116,15)", stack.Pop().ToString());
            Assert.AreEqual("search(157,163,24)", stack.Pop().ToString());
            Assert.AreEqual("engines(164,171,25)", stack.Pop().ToString());
        }

        [Test]
        public void Test1PhraseMVB()
        {
            MakeIndexLongMVB();

            FieldQuery fq = new FieldQuery(PqF("sp", "pe", "ee", "ed"), true, true); // "speed" -(2gram)-> "sp","pe","ee","ed"
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(4, stack.termList.Count);
            Assert.AreEqual("sp(88,90,61)", stack.Pop().ToString());
            Assert.AreEqual("pe(89,91,62)", stack.Pop().ToString());
            Assert.AreEqual("ee(90,92,63)", stack.Pop().ToString());
            Assert.AreEqual("ed(91,93,64)", stack.Pop().ToString());
        }
    }
}
