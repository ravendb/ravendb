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
using Lucene.Net.Index;
using Lucene.Net.Util;

using NUnit.Framework;

namespace Lucene.Net.Search.Vectorhighlight
{
    [TestFixture]
    public class IndexTimeSynonymTest : AbstractTestCase
    {
        [Test]
        public void TestFieldTermStackIndex1wSearch1term()
        {
            MakeIndex1w();

            FieldQuery fq = new FieldQuery(Tq("Mac"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("Mac(11,20,3)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex1wSearch2terms()
        {
            MakeIndex1w();

            BooleanQuery bq = new BooleanQuery();
            bq.Add(Tq("Mac"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            bq.Add(Tq("MacBook"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(bq, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(2, stack.termList.Count);
            Dictionary<String, String> expectedSet = new Dictionary<String, String>();
            expectedSet.Add("Mac(11,20,3)","");
            expectedSet.Add("MacBook(11,20,3)","");
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
        }

        [Test]
        public void TestFieldTermStackIndex1w2wSearch1term()
        {
            MakeIndex1w2w();

            FieldQuery fq = new FieldQuery(Tq("pc"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("pc(3,5,1)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex1w2wSearch1phrase()
        {
            MakeIndex1w2w();

            FieldQuery fq = new FieldQuery(PqF("personal", "computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(2, stack.termList.Count);
            Assert.AreEqual("personal(3,5,1)", stack.Pop().ToString());
            Assert.AreEqual("computer(3,5,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex1w2wSearch1partial()
        {
            MakeIndex1w2w();

            FieldQuery fq = new FieldQuery(Tq("computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("computer(3,5,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex1w2wSearch1term1phrase()
        {
            MakeIndex1w2w();

            BooleanQuery bq = new BooleanQuery();
            bq.Add(Tq("pc"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            bq.Add(PqF("personal", "computer"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(bq, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(3, stack.termList.Count);
            Dictionary<String, String> expectedSet = new Dictionary<String, String>();
            expectedSet.Add("pc(3,5,1)","");
            expectedSet.Add("personal(3,5,1)","");
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
            Assert.AreEqual("computer(3,5,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex2w1wSearch1term()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(Tq("pc"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("pc(3,20,1)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex2w1wSearch1phrase()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(PqF("personal", "computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(2, stack.termList.Count);
            Assert.AreEqual("personal(3,20,1)", stack.Pop().ToString());
            Assert.AreEqual("computer(3,20,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex2w1wSearch1partial()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(Tq("computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(1, stack.termList.Count);
            Assert.AreEqual("computer(3,20,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldTermStackIndex2w1wSearch1term1phrase()
        {
            MakeIndex2w1w();

            BooleanQuery bq = new BooleanQuery();
            bq.Add(Tq("pc"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            bq.Add(PqF("personal", "computer"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(bq, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            Assert.AreEqual(3, stack.termList.Count);
            Dictionary<String, String> expectedSet = new Dictionary<String, String>();
            expectedSet.Add("pc(3,20,1)","");
            expectedSet.Add("personal(3,20,1)","");
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
            Assert.IsTrue(expectedSet.ContainsKey(stack.Pop().ToString()));
            Assert.AreEqual("computer(3,20,2)", stack.Pop().ToString());
        }

        [Test]
        public void TestFieldPhraseListIndex1w2wSearch1phrase()
        {
            MakeIndex1w2w();

            FieldQuery fq = new FieldQuery(PqF("personal", "computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("personalcomputer(1.0)((3,5))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(5, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex1w2wSearch1partial()
        {
            MakeIndex1w2w();

            FieldQuery fq = new FieldQuery(Tq("computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("computer(1.0)((3,5))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(5, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex1w2wSearch1term1phrase()
        {
            MakeIndex1w2w();

            BooleanQuery bq = new BooleanQuery();
            bq.Add(Tq("pc"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            bq.Add(PqF("personal", "computer"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(bq, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.IsTrue(fpl.phraseList.First.Value.ToString().IndexOf("(1.0)((3,5))") > 0);
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(5, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex2w1wSearch1term()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(Tq("pc"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("pc(1.0)((3,20))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(20, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex2w1wSearch1phrase()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(PqF("personal", "computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("personalcomputer(1.0)((3,20))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(20, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex2w1wSearch1partial()
        {
            MakeIndex2w1w();

            FieldQuery fq = new FieldQuery(Tq("computer"), true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.AreEqual("computer(1.0)((3,20))", fpl.phraseList.First.Value.ToString());
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(20, fpl.phraseList.First.Value.GetEndOffset());
        }

        [Test]
        public void TestFieldPhraseListIndex2w1wSearch1term1phrase()
        {
            MakeIndex2w1w();

            BooleanQuery bq = new BooleanQuery();
            bq.Add(Tq("pc"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            bq.Add(PqF("personal", "computer"), Lucene.Net.Search.BooleanClause.Occur.SHOULD);
            FieldQuery fq = new FieldQuery(bq, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            Assert.AreEqual(1, fpl.phraseList.Count);
            Assert.IsTrue(fpl.phraseList.First.Value.ToString().IndexOf("(1.0)((3,20))") > 0);
            Assert.AreEqual(3, fpl.phraseList.First.Value.GetStartOffset());
            Assert.AreEqual(20, fpl.phraseList.First.Value.GetEndOffset());
        }

        private void MakeIndex1w()
        {
            //           11111111112
            // 012345678901234567890
            // I'll buy a Macintosh
            //            Mac
            //            MacBook
            // 0    1   2 3
            MakeSynonymIndex("I'll buy a Macintosh", 
                t("I'll", 0, 4),
                t("buy", 5, 8),
                t("a", 9, 10),
                t("Macintosh", 11, 20), t("Mac", 11, 20, 0), t("MacBook", 11, 20, 0));
        }

        private void MakeIndex1w2w()
        {
            //           1111111
            // 01234567890123456
            // My pc was broken
            //    personal computer
            // 0  1  2   3
            MakeSynonymIndex("My pc was broken",
                t("My", 0, 2),
                t("pc", 3, 5), t("personal", 3, 5, 0), t("computer", 3, 5),
                t("was", 6, 9),
                t("broken", 10, 16));
        }

        private void MakeIndex2w1w()
        {
            //           1111111111222222222233
            // 01234567890123456789012345678901
            // My personal computer was broken
            //    pc
            // 0  1        2        3   4
            MakeSynonymIndex("My personal computer was broken",
                t("My", 0, 2),
                t("personal", 3, 20), t("pc", 3, 20, 0), t("computer", 3, 20),
                t("was", 21, 24),
                t("broken", 25, 31));
        }

        void MakeSynonymIndex(String value, params Token[] tokens)
        {
            Analyzer analyzer = new TokenArrayAnalyzer(tokens);
            Make1dmfIndex(analyzer, value );
        }

        public static Token t(String text, int startOffset, int endOffset)
        {
            return t(text, startOffset, endOffset, 1);
        }

        public static Token t(String text, int startOffset, int endOffset, int positionIncrement)
        {
            Token token = new Token(text, startOffset, endOffset);
            token.SetPositionIncrement(positionIncrement);
            return token;
        }

        public class TokenArrayAnalyzer : Analyzer
        {
            Token[] tokens;
            public TokenArrayAnalyzer(Token[] tokens)
            {
                this.tokens = tokens;
            }

            public override TokenStream TokenStream(String fieldName, System.IO.TextReader reader)
            {
                Token reusableToken = new Token();

                Lucene.Net.Analysis.TokenStream.SetOnlyUseNewAPI(true);
                TokenStream ts = new AnonymousTokenStream(this, reusableToken);

                ts.AddAttributeImpl(reusableToken);
                return ts;
            }

            class AnonymousTokenStream : TokenStream
            {
                TokenArrayAnalyzer parent = null;
                Token reusableToken = null;

                public AnonymousTokenStream(TokenArrayAnalyzer parent,Token reusableToken)
                {
                    this.parent = parent;
                    this.reusableToken = reusableToken;
                }

                int p = 0;
                public override bool IncrementToken()
                {
                    if (p >= parent.tokens.Length) return false;
                    parent.tokens[p++].CopyTo(reusableToken);
                    return true;
                }
            }
        }
    }
}
