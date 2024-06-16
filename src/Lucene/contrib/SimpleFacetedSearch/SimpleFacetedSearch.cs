/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.QueryParsers;
using Lucene.Net.Store;
using Lucene.Net.Util;
using System.Threading;

#if !NET35
using System.Threading.Tasks;
#endif

/*
 Suppose, we want a faceted search on fields f1 f2 f3, 
 and their values in index are
 
          f1     f2     f3
          --     --     --
doc1      A      I      1  
doc2      A      I      2  
doc3      A      I      3  
doc4      A      J      1  
doc5      A      J      2  
doc6      A      J      3  
doc7      B      I      1  
 
 Algorithm:
 1- Find all possible values for f1 which are (A,B) , for f2 which are (I,J) and for f3 which are (1,2,3)
 2- Find Cartesian Product of (A,B)X(I,J)X(1,2,3). (12 possible groups)
 3- Eliminate the ones that surely result in 0 hits. (for ex, B J 2. since they have no doc. in common)
*/

/*
 TODO: Support for pre-built queries defining groups can be added 
*/

namespace Lucene.Net.Search
{
    public partial class SimpleFacetedSearch : IDisposable
    {
        public const int DefaultMaxDocPerGroup = 25;
        public static int MAX_FACETS = 2048;

        IndexReader _Reader;
        List<KeyValuePair<List<string>, OpenBitSetDISI>> _Groups = new List<KeyValuePair<List<string>, OpenBitSetDISI>>();
        
        public SimpleFacetedSearch(IndexReader reader, string groupByField) : this(reader, new string[] { groupByField })
        {
        }

        public SimpleFacetedSearch(IndexReader reader, string[] groupByFields)
        {
            this._Reader = reader;

            List<FieldValuesBitSets> fieldValuesBitSets = new List<FieldValuesBitSets>();

            //STEP 1
            //f1 = A, B
            //f2 = I, J
            //f3 = 1, 2, 3
            int maxFacets = 1;
            IList<IList<string>> inputToCP = new List<IList<string>>();
            foreach (string field in groupByFields)
            {
                FieldValuesBitSets f = new FieldValuesBitSets(reader, field);
                maxFacets *= f.FieldValueBitSetPair.Count;
                if (maxFacets > MAX_FACETS) throw new Exception("Facet count exceeded " + MAX_FACETS);
                fieldValuesBitSets.Add(f);
                inputToCP.Add(f.FieldValueBitSetPair.Keys.ToList());
            }

            //STEP 2
            // comb1: A I 1
            // comb2: A I 2 etc.
            var cp = inputToCP.CartesianProduct();

            //SETP 3
            //create a single BitSet for each combination
            //BitSet1: A AND I AND 1
            //BitSet2: A AND I AND 2 etc.
            //and remove impossible comb's (for ex, B J 3) from list.
#if !NET35
            Parallel.ForEach(cp, combinations =>
#else
            foreach(var combinations in cp)
#endif
            {
                OpenBitSetDISI bitSet = new OpenBitSetDISI(_Reader.MaxDoc);
                bitSet.Set(0, bitSet.Size());
                List<string> comb = combinations.ToList();

                for (int j = 0; j < comb.Count; j++)
                {
                    bitSet.And(fieldValuesBitSets[j].FieldValueBitSetPair[comb[j]]);
                }

                //STEP 3
                if (bitSet.Cardinality() > 0)
                {
                    lock(_Groups)
                        _Groups.Add(new KeyValuePair<List<string>, OpenBitSetDISI>(comb, bitSet));
                }
            }
#if !NET35
            );
#endif


            //Now _Groups has 7 rows (as <List<string>, BitSet> pairs) 
        }
        
        public Hits Search(Query query)
        {
            return Search(query, DefaultMaxDocPerGroup);
        }

        public Hits Search(Query query, int maxDocPerGroup)
        {
            var hitsPerGroup = new List<HitsPerFacet>();

            DocIdSet queryDocidSet = new CachingWrapperFilter(new QueryWrapperFilter(query)).GetDocIdSet(_Reader);
            var actions = new Action[_Groups.Count];           
            for (int i = 0; i < _Groups.Count; i++)
            {
                var h = new HitsPerFacet(new FacetName(_Groups[i].Key.ToArray()), _Reader, queryDocidSet, _Groups[i].Value, maxDocPerGroup);
                hitsPerGroup.Add(h);
                actions[i] = h.Calculate;
            }
            
#if !NET35
            Parallel.Invoke(actions);
#else
            foreach (var action in actions)
                action();
#endif
            
            Hits hits = new Hits {HitsPerFacet = hitsPerGroup.ToArray()};

            return hits;
        }

        public void Dispose()
        {
        }
    }
}
