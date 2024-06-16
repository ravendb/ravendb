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
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Search
{
    /// <summary>
    /// Subclass of FilteredTermEnum for enumerating a single term.
    /// <p/>
    /// This can be used by <see cref="MultiTermQuery"/>s that need only visit one term,
    /// but want to preserve MultiTermQuery semantics such as
    /// <see cref="RewriteMethod"/>.
    /// </summary>
    public class SingleTermEnum : FilteredTermEnum
    {
        private Term singleTerm;
        private bool _endEnum = false;

        /// <summary>
        /// Creates a new <c>SingleTermEnum</c>.
        /// <p/>
        /// After calling the constructor the enumeration is already pointing to the term,
        ///  if it exists.
        /// </summary>
        public SingleTermEnum(IndexReader reader, Term singleTerm, IState state)
        {
            this.singleTerm = singleTerm;
            SetEnum(reader.Terms(singleTerm, state), state);
        }

        public override float Difference()
        {
            return 1.0F;
        }

        public override bool EndEnum()
        {
            return _endEnum;
        }

        protected internal override bool TermCompare(Term term)
        {
            if (term.Equals(singleTerm))
            {
                return true;
            }
            else
            {
                _endEnum = true;
                return false;
            }
        }
    }
}
