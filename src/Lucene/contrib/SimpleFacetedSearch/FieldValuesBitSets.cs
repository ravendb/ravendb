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

namespace Lucene.Net.Search
{

    class FieldValuesBitSets
    {
        public string Field = "";
        public Dictionary<string, OpenBitSetDISI> FieldValueBitSetPair = new Dictionary<string, OpenBitSetDISI>();

        IndexReader _Reader;

        public FieldValuesBitSets(IndexReader reader, string field)
        {
            this.Field = field;
            this._Reader = reader;

            foreach (string val in GetFieldValues(field))
            {
                FieldValueBitSetPair.Add(val, GetBitSet(field, val));
            }
        }

        List<string> GetFieldValues(string groupByField)
        {
            List<string> list = new List<string>();
            TermEnum te = _Reader.Terms(new Term(groupByField, ""));
            if (te.Term.Field != groupByField) return list;
            list.Add(te.Term.Text);

            while (te.Next())
            {
                if (te.Term.Field != groupByField) break;
                list.Add(te.Term.Text);
            }
            return list;
        }

        OpenBitSetDISI GetBitSet(string groupByField, string group)
        {
            TermQuery query = new TermQuery(new Term(groupByField, group));
            Filter filter = new CachingWrapperFilter(new QueryWrapperFilter(query));
            return new OpenBitSetDISI(filter.GetDocIdSet(_Reader).Iterator(), _Reader.MaxDoc);
        }
    }
}
