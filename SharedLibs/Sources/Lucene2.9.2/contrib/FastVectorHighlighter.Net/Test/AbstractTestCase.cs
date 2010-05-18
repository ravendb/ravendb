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
    public abstract class AbstractTestCase
    {

        protected String F = "f";
        protected String F1 = "f1";
        protected String F2 = "f2";
        protected Directory dir;
        protected Analyzer analyzerW;
        protected Analyzer analyzerB;
        protected Analyzer analyzerK;
        protected IndexReader reader;
        protected QueryParser paW;
        protected QueryParser paB;

        protected static String[] shortMVValues = {
            "a b c",
            "",   // empty data in multi valued field
            "d e"
          };

        protected static String[] longMVValues = {
            "Followings are the examples of customizable parameters and actual examples of customization:",
            "The most search engines use only one of these methods. Even the search engines that says they can use the both methods basically"
          };

        // test data for LUCENE-1448 bug
        protected static String[] biMVValues = {
            "\nLucene/Solr does not require such additional hardware.",
            "\nWhen you talk about processing speed, the"
          };

        protected static String[] strMVValues = {
            "abc",
            "defg",
            "hijkl"
          };

        [SetUp]
        public void SetUp()
        {
            analyzerW = new WhitespaceAnalyzer();
            analyzerB = new BigramAnalyzer();
            analyzerK = new KeywordAnalyzer();
            paW = new QueryParser(F, analyzerW);
            paB = new QueryParser(F, analyzerB);
            dir = new RAMDirectory();
        }

        [TearDown]
        public void TearDown()
        {
            if (reader != null)
            {
                reader.Close();
                reader = null;
            }
        }

        protected Query Tq(String text)
        {
            return Tq(1F, text);
        }

        protected Query Tq(float boost, String text)
        {
            return Tq(boost, F, text);
        }

        protected Query Tq(String field, String text)
        {
            return Tq(1F, field, text);
        }

        protected Query Tq(float boost, String field, String text)
        {
            Query query = new TermQuery(new Term(field, text));
            query.SetBoost(boost);
            return query;
        }

        protected Query PqF(params String[] texts)
        {
            return PqF(1F, texts);
        }
        
        //protected Query pqF(String[] texts)
        //{
        //    return pqF(1F, texts);
        //}

        protected Query PqF(float boost, params String[] texts)
        {
            return pqF(boost, 0, texts);
        }

        protected Query pqF(float boost, int slop, params String[] texts)
        {
            return Pq(boost, slop, F, texts);
        }

        protected Query Pq(String field, params String[] texts)
        {
            return Pq(1F, 0, field, texts);
        }

        protected Query Pq(float boost, String field, params String[] texts)
        {
            return Pq(boost, 0, field, texts);
        }

        protected Query Pq(float boost, int slop, String field, params String[] texts)
        {
            PhraseQuery query = new PhraseQuery();
            foreach (String text in texts)
            {
                query.Add(new Term(field, text));
            }
            query.SetBoost(boost);
            query.SetSlop(slop);
            return query;
        }

        protected Query Dmq(params Query[] queries)
        {
            return Dmq(0.0F, queries);
        }

        protected Query Dmq(float tieBreakerMultiplier, params Query[] queries)
        {
            DisjunctionMaxQuery query = new DisjunctionMaxQuery(tieBreakerMultiplier);
            foreach (Query q in queries)
            {
                query.Add(q);
            }
            return query;
        }

        protected void AssertCollectionQueries(Dictionary<Query, Query> actual, params Query[] expected)
        {

            Assert.AreEqual(expected.Length, actual.Count);
            foreach (Query query in expected)
            {
                Assert.IsTrue(actual.ContainsKey(query));
            }
        }

        class BigramAnalyzer : Analyzer
        {
            public override TokenStream TokenStream(String fieldName, System.IO.TextReader reader)
            {
                return new BasicNGramTokenizer(reader);
            }
        }

        class BasicNGramTokenizer : Tokenizer
        {

            public static int DEFAULT_N_SIZE = 2;
            public static String DEFAULT_DELIMITERS = " \t\n.,";
            private int n;
            private String delimiters;
            private int startTerm;
            private int lenTerm;
            private int startOffset;
            private int nextStartOffset;
            private int ch;
            private String snippet;
            private StringBuilder snippetBuffer;
            private static int BUFFER_SIZE = 4096;
            private char[] charBuffer;
            private int charBufferIndex;
            private int charBufferLen;

            public BasicNGramTokenizer(System.IO.TextReader inReader): this(inReader, DEFAULT_N_SIZE)
            {
            }

            public BasicNGramTokenizer(System.IO.TextReader inReader, int n): this(inReader, n, DEFAULT_DELIMITERS)
            {
            }

            public BasicNGramTokenizer(System.IO.TextReader inReader, String delimiters) : this(inReader, DEFAULT_N_SIZE, delimiters)
            {
            }

            public BasicNGramTokenizer(System.IO.TextReader inReader, int n, String delimiters) : base(inReader)
            {
                this.n = n;
                this.delimiters = delimiters;
                startTerm = 0;
                nextStartOffset = 0;
                snippet = null;
                snippetBuffer = new StringBuilder();
                charBuffer = new char[BUFFER_SIZE];
                charBufferIndex = BUFFER_SIZE;
                charBufferLen = 0;
                ch = 0;

                Init();
            }

            void Init()
            {
                termAtt = (TermAttribute)AddAttribute(typeof(TermAttribute));
                offsetAtt = (OffsetAttribute)AddAttribute(typeof(OffsetAttribute));
            }

            TermAttribute termAtt = null;
            OffsetAttribute offsetAtt = null;

            public override bool IncrementToken()
            {
                if (!GetNextPartialSnippet())
                    return false;
                ClearAttributes();
                termAtt.SetTermBuffer(snippet, startTerm, lenTerm);
                offsetAtt.SetOffset(CorrectOffset(startOffset), CorrectOffset(startOffset + lenTerm));
                return true;
            }

            private int GetFinalOffset()
            {
                return nextStartOffset;
            }

            public override void End()
            {
                offsetAtt.SetOffset(GetFinalOffset(), GetFinalOffset());
            }

            protected bool GetNextPartialSnippet()
            {
                if (snippet != null && snippet.Length >= startTerm + 1 + n)
                {
                    startTerm++;
                    startOffset++;
                    lenTerm = n;
                    return true;
                }
                return GetNextSnippet();
            }

            protected bool GetNextSnippet()
            {
                startTerm = 0;
                startOffset = nextStartOffset;
                snippetBuffer.Remove(0, snippetBuffer.Length);
                while (true)
                {
                    if (ch != -1)
                        ch = ReadCharFromBuffer();
                    if (ch == -1) break;
                    else if (!IsDelimiter(ch))
                        snippetBuffer.Append((char)ch);
                    else if (snippetBuffer.Length > 0)
                        break;
                    else
                        startOffset++;
                }
                if (snippetBuffer.Length == 0)
                    return false;
                snippet = snippetBuffer.ToString();
                lenTerm = snippet.Length >= n ? n : snippet.Length;
                return true;
            }

            protected int ReadCharFromBuffer()
            {
                if (charBufferIndex >= charBufferLen)
                {
                    charBufferLen = input.Read(charBuffer,0,charBuffer.Length);
                    if (charBufferLen <= 0)
                    {
                        return -1;
                    }
                    charBufferIndex = 0;
                }
                int c = (int)charBuffer[charBufferIndex++];
                nextStartOffset++;
                return c;
            }

            protected bool IsDelimiter(int c)
            {
                return delimiters.IndexOf(Convert.ToChar(c) ) >= 0;
            }
        }

        protected void Make1d1fIndex(String value)
        {
            Make1dmfIndex( value );
        }

        protected void Make1d1fIndexB(String value)
        {
            Make1dmfIndexB( value );
        }

        protected void Make1dmfIndex(params String[] values)
        {
            Make1dmfIndex(analyzerW, values);
        }

        protected void Make1dmfIndexB(params String[] values)
        {
            Make1dmfIndex(analyzerB, values);
        }

        // make 1 doc with multi valued field
        protected void Make1dmfIndex(Analyzer analyzer, params String[] values)
        {
            IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
            Document doc = new Document();
            foreach (String value in values)
                doc.Add(new Field(F, value, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
            writer.AddDocument(doc);
            writer.Close();

            reader = IndexReader.Open(dir,true);
        }

        // make 1 doc with multi valued & not analyzed field
        protected void Make1dmfIndexNA(String[] values)
        {
            IndexWriter writer = new IndexWriter(dir, analyzerK, true, IndexWriter.MaxFieldLength.LIMITED);
            Document doc = new Document();
            foreach (String value in values)
                doc.Add(new Field(F, value, Field.Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
            writer.AddDocument(doc);
            writer.Close();

            reader = IndexReader.Open(dir, true);
        }

        protected void MakeIndexShortMV()
        {

            //  012345
            // "a b c"
            //  0 1 2

            // ""

            //  6789
            // "d e"
            //  3 4
            Make1dmfIndex(shortMVValues);
        }

        protected void MakeIndexLongMV()
        {
            //           11111111112222222222333333333344444444445555555555666666666677777777778888888888999
            // 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012
            // Followings are the examples of customizable parameters and actual examples of customization:
            // 0          1   2   3        4  5            6          7   8      9        10 11

            //        1                                                                                                   2
            // 999999900000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122
            // 345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901
            // The most search engines use only one of these methods. Even the search engines that says they can use the both methods basically
            // 12  13  (14)   (15)     16  17   18  19 20    21       22   23 (24)   (25)     26   27   28   29  30  31  32   33      34

            Make1dmfIndex(longMVValues);
        }

        protected void MakeIndexLongMVB()
        {
            // "*" [] LF

            //           1111111111222222222233333333334444444444555555
            // 01234567890123456789012345678901234567890123456789012345
            // *Lucene/Solr does not require such additional hardware.
            //  Lu 0        do 10    re 15   su 21       na 31
            //   uc 1        oe 11    eq 16   uc 22       al 32
            //    ce 2        es 12    qu 17   ch 23         ha 33
            //     en 3          no 13  ui 18     ad 24       ar 34
            //      ne 4          ot 14  ir 19     dd 25       rd 35
            //       e/ 5                 re 20     di 26       dw 36
            //        /S 6                           it 27       wa 37
            //         So 7                           ti 28       ar 38
            //          ol 8                           io 29       re 39
            //           lr 9                           on 30

            // 5555666666666677777777778888888888999999999
            // 6789012345678901234567890123456789012345678
            // *When you talk about processing speed, the
            //  Wh 40         ab 48     es 56         th 65
            //   he 41         bo 49     ss 57         he 66
            //    en 42         ou 50     si 58
            //       yo 43       ut 51     in 59
            //        ou 44         pr 52   ng 60
            //           ta 45       ro 53     sp 61
            //            al 46       oc 54     pe 62
            //             lk 47       ce 55     ee 63
            //                                    ed 64

            Make1dmfIndexB(biMVValues);
        }

        protected void MakeIndexStrMV()
        {
            //  0123
            // "abc"

            //  34567
            // "defg"

            //     111
            //  789012
            // "hijkl"
            Make1dmfIndexNA(strMVValues);
        }
    }
}
