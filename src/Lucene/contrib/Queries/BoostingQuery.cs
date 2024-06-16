/*
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
using System.Linq;
using System.Text;

using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Lucene.Net.Search
{
    /// <summary>
    ///  The BoostingQuery class can be used to effectively demote results that match a given query. 
    ///  Unlike the "NOT" clause, this still selects documents that contain undesirable terms, 
    ///  but reduces their overall score:
    ///  <pre>
    ///      Query balancedQuery = new BoostingQuery(positiveQuery, negativeQuery, 0.01f);
    ///  </pre>
    ///  In this scenario the positiveQuery contains the mandatory, desirable criteria which is used to 
    ///  select all matching documents, and the negativeQuery contains the undesirable elements which 
    ///  are simply used to lessen the scores. Documents that match the negativeQuery have their score 
    ///  multiplied by the supplied "boost" parameter, so this should be less than 1 to achieve a 
    ///  demoting effect
    ///  
    ///  This code was originally made available here:
    ///  <a href="http://marc.theaimsgroup.com/?l=lucene-user&amp;m=108058407130459&amp;w=2">mailing list</a>
    ///  and is documented here: <a href="http://wiki.apache.org/lucene-java/CommunityContributions">Documentation</a>
    /// </summary>
    public class BoostingQuery : Query
    {
        private float boost;                            // the amount to boost by
        private Query match;                            // query to match
        private Query context;                          // boost when matches too

        public BoostingQuery(Query match, Query context, float boost)
        {
            this.match = match;
            this.context = (Query)context.Clone();        // clone before boost
            this.boost = boost;

            this.context.Boost = 0.0f;                      // ignore context-only matches
        }

        public override Query Rewrite(IndexReader reader)
        {
            BooleanQuery result = new AnonymousBooleanQuery(boost);

            result.Add(match, Occur.MUST);
            result.Add(context, Occur.SHOULD);

            return result;
        }

        class AnonymousBooleanQuery : BooleanQuery
        {
            float boost;
            public AnonymousBooleanQuery(float boost)
            {
                this.boost = boost;
            }

            public override Similarity GetSimilarity(Searcher searcher)
            {
                return new AnonymousDefaultSimilarity(boost);
            }
        }

        class AnonymousDefaultSimilarity : DefaultSimilarity
        {
            float boost ;
            public AnonymousDefaultSimilarity(float boost)
            {
                this.boost = boost;
            }

            public override float Coord(int overlap, int max)
            {
                switch (overlap)
                {

                    case 1:                               // matched only one clause
                        return 1.0f;                        // use the score as-is

                    case 2:                               // matched both clauses
                        return boost;                       // multiply by boost

                    default:
                        return 0.0f;

                }
            }
        }
        
        public override  int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + BitConverter.ToInt32(BitConverter.GetBytes(boost),0);
            result = prime * result + ((context == null) ? 0 : context.GetHashCode());
            result = prime * result + ((match == null) ? 0 : match.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (this.GetType() != obj.GetType())
                return false;
            BoostingQuery other = (BoostingQuery)obj;
            if (BitConverter.ToInt32(BitConverter.GetBytes(boost),0) != BitConverter.ToInt32(BitConverter.GetBytes(other.boost),0) )
                return false;
            if (context == null)
            {
                if (other.context != null)
                    return false;
            }
            else if (!context.Equals(other.context))
                return false;
            if (match == null)
            {
                if (other.match != null)
                    return false;
            }
            else if (!match.Equals(other.match))
                return false;
            return true;
        }

        public override String ToString(String field)
        {
            return match.ToString(field) + "/" + context.ToString(field);
        }
    }
}
