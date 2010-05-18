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
using Occur = Lucene.Net.Search.BooleanClause.Occur;

using QueryPhraseMap = Lucene.Net.Search.Vectorhighlight.FieldQuery.QueryPhraseMap;
using TermInfo = Lucene.Net.Search.Vectorhighlight.FieldTermStack.TermInfo;

using NUnit.Framework;

namespace Lucene.Net.Search.Vectorhighlight
{
    [TestFixture]
    public class FieldQueryTest : AbstractTestCase
    {
        [Test]
        public void TestFlattenBoolean()
        {
            Query query = paW.Parse("A AND B OR C NOT (D AND E)");
            FieldQuery fq = new FieldQuery(query, true, true);
            HashSet<Query> flatQueries = new HashSet<Query>();
            fq.flatten(query, flatQueries);
            AssertCollectionQueries(flatQueries, Tq("A"), Tq("B"), Tq("C"));
        }

        [Test]
        public void testFlattenDisjunctionMaxQuery()
        {
            Query query = Dmq(Tq("A"), Tq("B"), PqF("C", "D"));
            FieldQuery fq = new FieldQuery(query, true, true);
            HashSet<Query> flatQueries = new HashSet<Query>();
            fq.flatten(query, flatQueries);
            AssertCollectionQueries(flatQueries, Tq("A"), Tq("B"), PqF("C", "D"));
        }

        [Test]
        public void TestFlattenTermAndPhrase()
        {
            Query query = paW.Parse("A AND \"B C\"");
            FieldQuery fq = new FieldQuery(query, true, true);
            HashSet<Query> flatQueries = new HashSet<Query>();
            fq.flatten(query, flatQueries);
            AssertCollectionQueries(flatQueries, Tq("A"), PqF("B", "C"));
        }

        [Test]
        public void TestFlattenTermAndPhrase2gram()
        {
            Query query = paB.Parse("AA AND BCD OR EFGH");
            FieldQuery fq = new FieldQuery(query, true, true);
            HashSet<Query> flatQueries = new HashSet<Query>();
            fq.flatten(query, flatQueries);
            AssertCollectionQueries(flatQueries, Tq("AA"), PqF("BC", "CD" ), PqF("EF", "FG", "GH"));
        }

        [Test]
        public void TestFlatten1TermPhrase()
        {
            Query query = PqF("A");
            FieldQuery fq = new FieldQuery(query, true, true);
            HashSet<Query> flatQueries = new HashSet<Query>();
            fq.flatten(query, flatQueries);
            AssertCollectionQueries(flatQueries, Tq("A"));
        }

        [Test]
        public void TestExpand()
        {
            Query dummy = PqF("DUMMY");
            FieldQuery fq = new FieldQuery(dummy, true, true);

            // "a b","b c" => "a b","b c","a b c"
            HashSet<Query> flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF( "b", "c" ));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("b", "c"), PqF("a", "b", "c"));

            // "a b","b c d" => "a b","b c d","a b c d"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF("b", "c", "d"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("b", "c", "d"), PqF("a", "b", "c", "d"));

            // "a b c","b c d" => "a b c","b c d","a b c d"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "c"));
            flatQueries.Add(PqF("b", "c", "d"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "c"), PqF("b", "c", "d"), PqF("a", "b", "c", "d"));

            // "a b c","c d e" => "a b c","c d e","a b c d e"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "c"));
            flatQueries.Add(PqF("c", "d", "e"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "c"), PqF("c", "d", "e"), PqF("a", "b", "c", "d", "e"));

            // "a b c d","b c" => "a b c d","b c"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "c", "d"));
            flatQueries.Add(PqF("b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "c", "d"), PqF("b", "c"));

            // "a b b","b c" => "a b b","b c","a b b c"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "b"));
            flatQueries.Add(PqF("b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "b"), PqF("b", "c"), PqF("a", "b", "b", "c"));

            // "a b","b a" => "a b","b a","a b a", "b a b"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF("b", "a"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("b", "a"), PqF("a", "b", "a"), PqF("b", "a", "b"));

            // "a b","a b c" => "a b","a b c"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF("a", "b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("a", "b", "c"));
        }

        [Test]
        public void TestNoExpand()
        {
            Query dummy = PqF("DUMMY");
            FieldQuery fq = new FieldQuery(dummy, true, true);

            // "a b","c d" => "a b","c d"
            HashSet<Query> flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF("c", "d"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("c", "d"));

            // "a","a b" => "a", "a b"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(Tq("a"));
            flatQueries.Add(PqF("a", "b"));
            AssertCollectionQueries(fq.expand(flatQueries),
                Tq("a"), PqF("a", "b"));

            // "a b","b" => "a b", "b"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(Tq("b"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), Tq("b"));

            // "a b c","b c" => "a b c","b c"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "c"));
            flatQueries.Add(PqF("b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "c"), PqF("b", "c"));

            // "a b","a b c" => "a b","a b c"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b"));
            flatQueries.Add(PqF("a", "b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b"), PqF("a", "b", "c"));

            // "a b c","b d e" => "a b c","b d e"
            flatQueries = new HashSet<Query>();
            flatQueries.Add(PqF("a", "b", "c"));
            flatQueries.Add(PqF("b", "d", "e"));
            AssertCollectionQueries(fq.expand(flatQueries),
                PqF("a", "b", "c"), PqF("b", "d", "e"));
        }

        [Test]
        public void TestExpandNotFieldMatch()
        {
            Query dummy = PqF("DUMMY");
            FieldQuery fq = new FieldQuery(dummy, true, false);

            // f1:"a b",f2:"b c" => f1:"a b",f2:"b c",f1:"a b c"
            HashSet<Query> flatQueries = new HashSet<Query>();
            flatQueries.Add(Pq(F1, "a", "b"));
            flatQueries.Add(Pq(F2, "b", "c"));
            AssertCollectionQueries(fq.expand(flatQueries),
                Pq(F1, "a", "b"), Pq(F2, "b", "c"), Pq(F1, "a", "b", "c"));
        }

        [Test]
        public void TestGetFieldTermMap()
        {
            Query query = Tq("a");
            FieldQuery fq = new FieldQuery(query, true, true);

            QueryPhraseMap pqm = fq.GetFieldTermMap(F, "a");
            Assert.NotNull(pqm);
            Assert.IsTrue(pqm.IsTerminal());

            pqm = fq.GetFieldTermMap(F, "b");
            Assert.Null(pqm);

            pqm = fq.GetFieldTermMap(F1, "a");
            Assert.Null(pqm);
        }

        [Test]
        public void TestGetRootMap()
        {
            Query dummy = PqF("DUMMY");
            FieldQuery fq = new FieldQuery(dummy, true, true);

            QueryPhraseMap rootMap1 = fq.getRootMap(Tq("a"));
            QueryPhraseMap rootMap2 = fq.getRootMap(Tq("a"));
            Assert.IsTrue(rootMap1 == rootMap2);
            QueryPhraseMap rootMap3 = fq.getRootMap(Tq("b"));
            Assert.IsTrue(rootMap1 == rootMap3);
            QueryPhraseMap rootMap4 = fq.getRootMap(Tq(F1, "b"));
            Assert.IsFalse(rootMap4 == rootMap3);
        }

        [Test]
        public void TestGetRootMapNotFieldMatch()
        {
            Query dummy = PqF("DUMMY");
            FieldQuery fq = new FieldQuery(dummy, true, false);

            QueryPhraseMap rootMap1 = fq.getRootMap(Tq("a"));
            QueryPhraseMap rootMap2 = fq.getRootMap(Tq("a"));
            Assert.IsTrue(rootMap1 == rootMap2);
            QueryPhraseMap rootMap3 = fq.getRootMap(Tq("b"));
            Assert.IsTrue(rootMap1 == rootMap3);
            QueryPhraseMap rootMap4 = fq.getRootMap(Tq(F1, "b"));
            Assert.IsTrue(rootMap4 == rootMap3);
        }

        [Test]
        public void TestGetTermSet()
        {
            Query query = paW.Parse("A AND B OR x:C NOT (D AND E)");
            FieldQuery fq = new FieldQuery(query, true, true);
            Assert.AreEqual(2, fq.termSetMap.Count);
            List<String> termSet = fq.getTermSet(F);
            Assert.AreEqual(2, termSet.Count);
            Assert.IsTrue(termSet.Contains("A"));
            Assert.IsTrue(termSet.Contains("B"));
            termSet = fq.getTermSet("x");
            Assert.AreEqual(1, termSet.Count);
            Assert.IsTrue(termSet.Contains("C"));
            termSet = fq.getTermSet("y");
            Assert.Null(termSet);
        }

        [Test]
        public void TestQueryPhraseMap1Term()
        {
            Query query = Tq("a");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            HashMap<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.IsTrue(qpm.subMap.Get("a") != null);
            Assert.IsTrue(qpm.subMap.Get("a").terminal);
            Assert.AreEqual(1F, qpm.subMap.Get("a").boost);

            // phraseHighlight = true, fieldMatch = false
            fq = new FieldQuery(query, true, false);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(F));
            Assert.NotNull(map.Get(null));
            qpm = map.Get(null);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.IsTrue(qpm.subMap.Get("a") != null);
            Assert.IsTrue(qpm.subMap.Get("a").terminal);
            Assert.AreEqual(1F, qpm.subMap.Get("a").boost);

            // phraseHighlight = false, fieldMatch = true
            fq = new FieldQuery(query, false, true);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.IsTrue(qpm.subMap.Get("a") != null);
            Assert.IsTrue(qpm.subMap.Get("a").terminal);
            Assert.AreEqual(1F, qpm.subMap.Get("a").boost);

            // phraseHighlight = false, fieldMatch = false
            fq = new FieldQuery(query, false, false);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(F));
            Assert.NotNull(map.Get(null));
            qpm = map.Get(null);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.IsTrue(qpm.subMap.Get("a") != null);
            Assert.IsTrue(qpm.subMap.Get("a").terminal);
            Assert.AreEqual(1F, qpm.subMap.Get("a").boost);

            // boost != 1
            query = Tq(2, "a");
            fq = new FieldQuery(query, true, true);
            map = fq.rootMaps;
            qpm = map.Get(F);
            Assert.AreEqual(2F, qpm.subMap.Get("a").boost);
        }

        [Test]
        public void TestQueryPhraseMap1Phrase()
        {
            Query query = PqF("a", "b");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            HashMap<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // phraseHighlight = true, fieldMatch = false
            fq = new FieldQuery(query, true, false);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count); 
            Assert.Null(map.Get(F));
            Assert.NotNull(map.Get(null));
            qpm = map.Get(null);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("a"));
            qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // phraseHighlight = false, fieldMatch = true
            fq = new FieldQuery(query, false, true);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            qpm = map.Get(F);
            Assert.AreEqual(2, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("a"));
            qpm2 = qpm.subMap.Get("a");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            Assert.NotNull(qpm.subMap.Get("b"));
            qpm2 = qpm.subMap.Get("b");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);

            // phraseHighlight = false, fieldMatch = false
            fq = new FieldQuery(query, false, false);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(F));
            Assert.NotNull(map.Get(null));
            qpm = map.Get(null);
            Assert.AreEqual(2, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("a"));
            qpm2 = qpm.subMap.Get("a");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            Assert.NotNull(qpm.subMap.Get("b"));
            qpm2 = qpm.subMap.Get("b");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);

            // boost != 1
            query = PqF(2, "a", "b");
            // phraseHighlight = false, fieldMatch = false
            fq = new FieldQuery(query, false, false);
            map = fq.rootMaps;
            qpm = map.Get(null);
            qpm2 = qpm.subMap.Get("a");
            Assert.AreEqual(2F, qpm2.boost);
            qpm3 = qpm2.subMap.Get("b");
            Assert.AreEqual(2F, qpm3.boost);
            qpm2 = qpm.subMap.Get("b");
            Assert.AreEqual(2F, qpm2.boost);
        }

        [Test]
        public void TestQueryPhraseMap1PhraseAnother()
        {
            Query query = PqF("search", "engines");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("search"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("search");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("engines"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("engines");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);
        }

        [Test]
        public void TestQueryPhraseMap2Phrases()
        {
            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "b"), Occur.SHOULD);
            query.Add(PqF(2, "c", "d"), Occur.SHOULD);

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(2, qpm.subMap.Count);

            // "a b"
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "c d"^2
            Assert.NotNull(qpm.subMap.Get("c"));
            qpm2 = qpm.subMap.Get("c");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("d"));
            qpm3 = qpm2.subMap.Get("d");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(2F, qpm3.boost);
        }

        [Test]
        public void TestQueryPhraseMap2PhrasesFields()
        {
            BooleanQuery query = new BooleanQuery();
            query.Add(Pq(F1, "a", "b"), Occur.SHOULD);
            query.Add(Pq(2F, F2, "c", "d"), Occur.SHOULD);

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            HashMap<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(2, map.Count);
            Assert.Null(map.Get(null));

            // "a b"
            Assert.NotNull(map.Get(F1));
            QueryPhraseMap qpm = map.Get(F1);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "c d"^2
            Assert.NotNull(map.Get(F2));
            qpm = map.Get(F2);
            Assert.AreEqual(1, qpm.subMap.Count);
            Assert.NotNull(qpm.subMap.Get("c"));
            qpm2 = qpm.subMap.Get("c");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("d"));
            qpm3 = qpm2.subMap.Get("d");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(2F, qpm3.boost);

            // phraseHighlight = true, fieldMatch = false
            fq = new FieldQuery(query, true, false);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(F1));
            Assert.Null(map.Get(F2));
            Assert.NotNull(map.Get(null));
            qpm = map.Get(null);
            Assert.AreEqual(2, qpm.subMap.Count);

            // "a b"
            Assert.NotNull(qpm.subMap.Get("a"));
            qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "c d"^2
            Assert.NotNull(qpm.subMap.Get("c"));
            qpm2 = qpm.subMap.Get("c");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("d"));
            qpm3 = qpm2.subMap.Get("d");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(2F, qpm3.boost);
        }

        /*
         * <t>...terminal
         * 
         * a-b-c-<t>
         *     +-d-<t>
         * b-c-d-<t>
         * +-d-<t>
         */
        [Test]
        public void TestQueryPhraseMapOverlapPhrases()
        {
            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "b", "c"), Occur.SHOULD);
            query.Add(PqF(2, "b", "c", "d"), Occur.SHOULD);
            query.Add(PqF(3, "b", "d"), Occur.SHOULD);

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(2, qpm.subMap.Count);

            // "a b c"
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("b");
            Assert.IsFalse(qpm3.terminal);
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("c"));
            QueryPhraseMap qpm4 = qpm3.subMap.Get("c");
            Assert.IsTrue(qpm4.terminal);
            Assert.AreEqual(1F, qpm4.boost);
            Assert.NotNull(qpm4.subMap.Get("d"));
            QueryPhraseMap qpm5 = qpm4.subMap.Get("d");
            Assert.IsTrue(qpm5.terminal);
            Assert.AreEqual(1F, qpm5.boost);

            // "b c d"^2, "b d"^3
            Assert.NotNull(qpm.subMap.Get("b"));
            qpm2 = qpm.subMap.Get("b");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(2, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("c"));
            qpm3 = qpm2.subMap.Get("c");
            Assert.IsFalse(qpm3.terminal);
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("d"));
            qpm4 = qpm3.subMap.Get("d");
            Assert.IsTrue(qpm4.terminal);
            Assert.AreEqual(2F, qpm4.boost);
            Assert.NotNull(qpm2.subMap.Get("d"));
            qpm3 = qpm2.subMap.Get("d");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(3F, qpm3.boost);
        }

        /*
         * <t>...terminal
         * 
         * a-b-<t>
         *   +-c-<t>
         */
        [Test]
        public void TestQueryPhraseMapOverlapPhrases2()
        {
            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "b"), Occur.SHOULD);
            query.Add(PqF(2, "a", "b", "c"), Occur.SHOULD);

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);

            // "a b"
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("b"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("b");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "a b c"^2
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("c"));
            QueryPhraseMap qpm4 = qpm3.subMap.Get("c");
            Assert.IsTrue(qpm4.terminal);
            Assert.AreEqual(2F, qpm4.boost);
        }

        /*
         * <t>...terminal
         * 
         * a-a-a-<t>
         *     +-a-<t>
         *       +-a-<t>
         *         +-a-<t>
         */
        [Test]
        public void TestQueryPhraseMapOverlapPhrases3()
        {
            BooleanQuery query = new BooleanQuery();
            query.Add(PqF("a", "a", "a", "a"), Occur.SHOULD);
            query.Add(PqF(2, "a", "a", "a"), Occur.SHOULD);

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(1, qpm.subMap.Count);

            // "a a a"
            Assert.NotNull(qpm.subMap.Get("a"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("a");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("a"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("a");
            Assert.IsFalse(qpm3.terminal);
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("a"));
            QueryPhraseMap qpm4 = qpm3.subMap.Get("a");
            Assert.IsTrue(qpm4.terminal);

            // "a a a a"
            Assert.AreEqual(1, qpm4.subMap.Count);
            Assert.NotNull(qpm4.subMap.Get("a"));
            QueryPhraseMap qpm5 = qpm4.subMap.Get("a");
            Assert.IsTrue(qpm5.terminal);

            // "a a a a a"
            Assert.AreEqual(1, qpm5.subMap.Count);
            Assert.NotNull(qpm5.subMap.Get("a"));
            QueryPhraseMap qpm6 = qpm5.subMap.Get("a");
            Assert.IsTrue(qpm6.terminal);

            // "a a a a a a"
            Assert.AreEqual(1, qpm6.subMap.Count);
            Assert.NotNull(qpm6.subMap.Get("a"));
            QueryPhraseMap qpm7 = qpm6.subMap.Get("a");
            Assert.IsTrue(qpm7.terminal);
        }

        [Test]
        public void TestQueryPhraseMapOverlap2gram()
        {
            Query query = paB.Parse("abc AND bcd");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);
            Dictionary<String, QueryPhraseMap> map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            QueryPhraseMap qpm = map.Get(F);
            Assert.AreEqual(2, qpm.subMap.Count);

            // "ab bc"
            Assert.NotNull(qpm.subMap.Get("ab"));
            QueryPhraseMap qpm2 = qpm.subMap.Get("ab");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("bc"));
            QueryPhraseMap qpm3 = qpm2.subMap.Get("bc");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "ab bc cd"
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("cd"));
            QueryPhraseMap qpm4 = qpm3.subMap.Get("cd");
            Assert.IsTrue(qpm4.terminal);
            Assert.AreEqual(1F, qpm4.boost);

            // "bc cd"
            Assert.NotNull(qpm.subMap.Get("bc"));
            qpm2 = qpm.subMap.Get("bc");
            Assert.IsFalse(qpm2.terminal);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("cd"));
            qpm3 = qpm2.subMap.Get("cd");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // phraseHighlight = false, fieldMatch = true
            fq = new FieldQuery(query, false, true);
            map = fq.rootMaps;
            Assert.AreEqual(1, map.Count);
            Assert.Null(map.Get(null));
            Assert.NotNull(map.Get(F));
            qpm = map.Get(F);
            Assert.AreEqual(3, qpm.subMap.Count);

            // "ab bc"
            Assert.NotNull(qpm.subMap.Get("ab"));
            qpm2 = qpm.subMap.Get("ab");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("bc"));
            qpm3 = qpm2.subMap.Get("bc");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "ab bc cd"
            Assert.AreEqual(1, qpm3.subMap.Count);
            Assert.NotNull(qpm3.subMap.Get("cd"));
            qpm4 = qpm3.subMap.Get("cd");
            Assert.IsTrue(qpm4.terminal);
            Assert.AreEqual(1F, qpm4.boost);

            // "bc cd"
            Assert.NotNull(qpm.subMap.Get("bc"));
            qpm2 = qpm.subMap.Get("bc");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);
            Assert.AreEqual(1, qpm2.subMap.Count);
            Assert.NotNull(qpm2.subMap.Get("cd"));
            qpm3 = qpm2.subMap.Get("cd");
            Assert.IsTrue(qpm3.terminal);
            Assert.AreEqual(1F, qpm3.boost);

            // "cd"
            Assert.NotNull(qpm.subMap.Get("cd"));
            qpm2 = qpm.subMap.Get("cd");
            Assert.IsTrue(qpm2.terminal);
            Assert.AreEqual(1F, qpm2.boost);
            Assert.AreEqual(0, qpm2.subMap.Count);
        }

        [Test]
        public void TestSearchPhrase()
        {
            Query query = PqF("a", "b", "c");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);

            // "a"
            List<TermInfo> phraseCandidate = new List<TermInfo>();
            phraseCandidate.Add(new TermInfo("a", 0, 1, 0));
            Assert.Null(fq.SearchPhrase(F, phraseCandidate));
            // "a b"
            phraseCandidate.Add(new TermInfo("b", 2, 3, 1));
            Assert.Null(fq.SearchPhrase(F, phraseCandidate));
            // "a b c"
            phraseCandidate.Add(new TermInfo("c", 4, 5, 2));
            Assert.NotNull(fq.SearchPhrase(F, phraseCandidate));
            Assert.Null(fq.SearchPhrase("x", phraseCandidate));

            // phraseHighlight = true, fieldMatch = false
            fq = new FieldQuery(query, true, false);

            // "a b c"
            Assert.NotNull(fq.SearchPhrase(F, phraseCandidate)); //{{DIGY - Failing test.}}
            Assert.NotNull(fq.SearchPhrase("x", phraseCandidate)); //{{DIGY - Failing test.}}
            //{{DIGY- this may be related with the difference of List implemantation between Java & .NET
            //Java version accepts "null" as a value. It is not a show stopper.}}

            // phraseHighlight = false, fieldMatch = true
            fq = new FieldQuery(query, false, true);

            // "a"
            phraseCandidate.Clear();
            phraseCandidate.Add(new TermInfo("a", 0, 1, 0));
            Assert.NotNull(fq.SearchPhrase(F, phraseCandidate));
            // "a b"
            phraseCandidate.Add(new TermInfo("b", 2, 3, 1));
            Assert.Null(fq.SearchPhrase(F, phraseCandidate));
            // "a b c"
            phraseCandidate.Add(new TermInfo("c", 4, 5, 2));
            Assert.NotNull(fq.SearchPhrase(F, phraseCandidate));
            Assert.Null(fq.SearchPhrase("x", phraseCandidate));
        }

        [Test]
        public void TestSearchPhraseSlop()
        {
            // "a b c"~0
            Query query = PqF("a", "b", "c");

            // phraseHighlight = true, fieldMatch = true
            FieldQuery fq = new FieldQuery(query, true, true);

            // "a b c" w/ position-gap = 2
            List<TermInfo> phraseCandidate = new List<TermInfo>();
            phraseCandidate.Add(new TermInfo("a", 0, 1, 0));
            phraseCandidate.Add(new TermInfo("b", 2, 3, 2));
            phraseCandidate.Add(new TermInfo("c", 4, 5, 4));
            Assert.Null(fq.SearchPhrase(F, phraseCandidate));

            // "a b c"~1
            query = pqF(1F, 1, "a", "b", "c");

            // phraseHighlight = true, fieldMatch = true
            fq = new FieldQuery(query, true, true);

            // "a b c" w/ position-gap = 2
            Assert.NotNull(fq.SearchPhrase(F, phraseCandidate));

            // "a b c" w/ position-gap = 3
            phraseCandidate.Clear();
            phraseCandidate.Add(new TermInfo("a", 0, 1, 0));
            phraseCandidate.Add(new TermInfo("b", 2, 3, 3));
            phraseCandidate.Add(new TermInfo("c", 4, 5, 6));
            Assert.Null(fq.SearchPhrase(F, phraseCandidate));
        }
    }

}
