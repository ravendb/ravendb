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
    public class ScoreOrderFragmentsBuilderTest : AbstractTestCase
    {

        [Test]
        public void Test3Frags()
        {
            FieldFragList ffl =  this.Ffl("a c", "a b b b b b b b b b b b a b a b b b b b c a a b b");
            ScoreOrderFragmentsBuilder sofb = new ScoreOrderFragmentsBuilder();
            String[] f = sofb.CreateFragments(reader, 0, F, ffl, 3);
            Assert.AreEqual(3, f.Length);
            // check score order
            Assert.AreEqual("<b>c</b> <b>a</b> <b>a</b> b b", f[0]);
            Assert.AreEqual("b b <b>a</b> b <b>a</b> b b b b b ", f[1]);
            Assert.AreEqual("<b>a</b> b b b b b b b b b ", f[2]);
        }

        private FieldFragList Ffl(String queryValue, String indexValue)
        {
            Make1d1fIndex(indexValue);
            Query query = paW.Parse(queryValue);
            FieldQuery fq = new FieldQuery(query, true, true);
            FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
            FieldPhraseList fpl = new FieldPhraseList(stack, fq);
            return new SimpleFragListBuilder().CreateFieldFragList(fpl, 20);
        }
    }
}
