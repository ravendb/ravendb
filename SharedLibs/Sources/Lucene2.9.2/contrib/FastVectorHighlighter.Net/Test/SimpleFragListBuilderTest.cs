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


using Lucene.Net.Search;

using NUnit.Framework;

namespace Lucene.Net.Search.Vectorhighlight
{
    [TestFixture]
    public class SimpleFragListBuilderTest : AbstractTestCase
    {

        [Test]
        public void TestNullFieldFragList()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("a", "b c d"), 100);
            Assert.AreEqual(0, ffl.fragInfos.Count);
        }

        [Test]
        public void TestTooSmallFragSize()
        {
            try
            {
                SimpleFragListBuilder sflb = new SimpleFragListBuilder();
                sflb.CreateFieldFragList(fpl("a", "b c d"), SimpleFragListBuilder.MIN_FRAG_CHAR_SIZE - 1);
                Assert.Fail("IllegalArgumentException must be thrown");
            }
            catch (ArgumentException)
            {
            }
        }

        [Test]
        public void TestSmallerFragSizeThanTermQuery()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("abcdefghijklmnopqrs", "abcdefghijklmnopqrs"), SimpleFragListBuilder.MIN_FRAG_CHAR_SIZE);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(abcdefghijklmnopqrs((0,19)))/1.0(0,19)", ffl.fragInfos[0].ToString());
        }

        [Test]
        public void TestSmallerFragSizeThanPhraseQuery() {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.CreateFieldFragList( fpl( "\"abcdefgh jklmnopqrs\"", "abcdefgh   jklmnopqrs" ), SimpleFragListBuilder.MIN_FRAG_CHAR_SIZE );
    Assert.AreEqual( 1, ffl.fragInfos.Count );
    Console.WriteLine( ffl.fragInfos[ 0 ].ToString() );
    Assert.AreEqual( "subInfos=(abcdefghjklmnopqrs((0,21)))/1.0(0,21)", ffl.fragInfos[0] .ToString() );
  }

        [Test]
        public void Test1TermIndex()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("a", "a"), 100);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1)))/1.0(0,100)", ffl.fragInfos[0].ToString());
        }

        [Test]
        public void Test2TermsIndex1Frag()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("a", "a a"), 100);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1))a((2,3)))/2.0(0,100)", ffl.fragInfos[0].ToString());

            ffl = sflb.CreateFieldFragList(fpl("a", "a b b b b b b b b a"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1))a((18,19)))/2.0(0,20)", ffl.fragInfos[0].ToString());

            ffl = sflb.CreateFieldFragList(fpl("a", "b b b b a b b b b a"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((8,9))a((18,19)))/2.0(2,22)", ffl.fragInfos[0].ToString());
        }

        [Test]
        public void Test2TermsIndex2Frags()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("a", "a b b b b b b b b b b b b b a"), 20);
            Assert.AreEqual(2, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1)))/1.0(0,20)", ffl.fragInfos[0].ToString());
            Assert.AreEqual("subInfos=(a((28,29)))/1.0(22,42)", ffl.fragInfos[1].ToString());

            ffl = sflb.CreateFieldFragList(fpl("a", "a b b b b b b b b b b b b a"), 20);
            Assert.AreEqual(2, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1)))/1.0(0,20)", ffl.fragInfos[0].ToString());
            Assert.AreEqual("subInfos=(a((26,27)))/1.0(20,40)", ffl.fragInfos[1].ToString());

            ffl = sflb.CreateFieldFragList(fpl("a", "a b b b b b b b b b a"), 20);
            Assert.AreEqual(2, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1)))/1.0(0,20)", ffl.fragInfos[0].ToString());
            Assert.AreEqual("subInfos=(a((20,21)))/1.0(20,40)", ffl.fragInfos[1].ToString());
        }

        [Test]
        public void Test2TermsQuery()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("a b", "c d e"), 20);
            Assert.AreEqual(0, ffl.fragInfos.Count);

            ffl = sflb.CreateFieldFragList(fpl("a b", "d b c"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(b((2,3)))/1.0(0,20)", ffl.fragInfos[0].ToString());

            ffl = sflb.CreateFieldFragList(fpl("a b", "a b c"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(a((0,1))b((2,3)))/2.0(0,20)", ffl.fragInfos[0].ToString());
        }

        [Test]
        public void TestPhraseQuery()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("\"a b\"", "c d e"), 20);
            Assert.AreEqual(0, ffl.fragInfos.Count);

            ffl = sflb.CreateFieldFragList(fpl("\"a b\"", "a c b"), 20);
            Assert.AreEqual(0, ffl.fragInfos.Count);

            ffl = sflb.CreateFieldFragList(fpl("\"a b\"", "a b c"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(ab((0,3)))/1.0(0,20)", ffl.fragInfos[0].ToString());
        }

        [Test]
        public void TestPhraseQuerySlop()
        {
            SimpleFragListBuilder sflb = new SimpleFragListBuilder();
            FieldFragList ffl = sflb.CreateFieldFragList(fpl("\"a b\"~1", "a c b"), 20);
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(ab((0,1)(4,5)))/1.0(0,20)", ffl.fragInfos[0].ToString());
        }

        
        private FieldPhraseList fpl(String queryValue, String indexValue)
        {
            Make1d1fIndex(indexValue);
            Query query = paW.Parse(queryValue);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            return new FieldPhraseList(stack, fq);
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
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(d((6,7)))/1.0(0,100)", ffl.fragInfos[0].ToString());
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
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(searchengines((102,116))searchengines((157,171)))/2.0(96,196)", ffl.fragInfos[0].ToString());
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
            Assert.AreEqual(1, ffl.fragInfos.Count);
            Assert.AreEqual("subInfos=(sppeeeed((88,93)))/1.0(82,182)", ffl.fragInfos[0].ToString());
        }
    }
}
