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
using Lucene.Net.Util;

namespace Lucene.Net.Search
{
    /// <summary>
    /// A filter that contains multiple terms.
    /// </summary>
    public class TermsFilter : Filter
    {
        /// <summary>
        /// The set of terms for this filter.
        /// </summary>
        protected ISet<Term> terms = new SortedSet<Term>();

        /// <summary>
        /// Add a term to the set.
        /// </summary>
        /// <param name="term">The term to add.</param>
        public void AddTerm(Term term)
        {
            terms.Add(term);
        }

        /// <summary>
        /// Get the DocIdSet.
        /// </summary>
        /// <param name="reader">Applcible reader.</param>
        /// <returns>The set.</returns>
        public override DocIdSet GetDocIdSet(IndexReader reader)
        {
            OpenBitSet result = new OpenBitSet(reader.MaxDoc);
            TermDocs td = reader.TermDocs();
            try
            {
                foreach (Term t in this.terms)
                {
                    td.Seek(t);
                    while (td.Next())
                    {
                        result.Set(td.Doc);
                    }
                }
            }
            finally
            {
                td.Close();
            }

            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if ((obj == null) || !(obj is TermsFilter))
            {
                return false;
            }
            TermsFilter test = (TermsFilter)obj;
            // TODO: Does SortedSet have an issues like List<T>?  see EquatableList in Support
            return (terms == test.terms || (terms != null && terms.Equals(test.terms)));
        }

        public override int GetHashCode()
        {
            int hash = 9;
            foreach (Term t in this.terms)
            {
                hash = 31 * hash + t.GetHashCode();
            }
            return hash;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("(");
            foreach (Term t in this.terms)
            {
                sb.AppendFormat(" {0}:{1}", t.Field, t.Text);
            }
            sb.Append(" )");
            return sb.ToString();
        }
    }
}
