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

using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Search
{
    /// <summary>
    /// Subclass of FilteredTermEnum for enumerating all terms that match the
    /// specified regular expression term using the specified regular expression
    /// implementation.
    /// <para>Term enumerations are always ordered by Term.compareTo().  Each term in
    /// the enumeration is greater than all that precede it.</para>
    /// </summary>
    /// <remarks>http://www.java2s.com/Open-Source/Java-Document/Net/lucene-connector/org/apache/lucene/search/regex/RegexTermEnum.java.htm</remarks>
    public class RegexTermEnum : FilteredTermEnum
    {
        private readonly string _sField;
        private bool _bEndEnum;
        private System.Text.RegularExpressions.Regex _regex;

        public RegexTermEnum(IndexReader reader, Term term, IState state, System.Text.RegularExpressions.Regex regex)
        {
            _sField = term.Field;

            _regex = regex;

            SetEnum(reader.Terms(new Term(term.Field, string.Empty), state), state);
        }

        /// <summary>Equality compare on the term </summary>
        protected override bool TermCompare(Term term)
        {
            if (_sField == term.Field)
            {
                return _regex.IsMatch(term.Text);
            } //eif

            _bEndEnum = true;
            return false;
        }

        /// <summary>Equality measure on the term </summary>
        public override float Difference()
        {
            return 1.0F;
        }

        /// <summary>Indicates the end of the enumeration has been reached </summary>
        public override bool EndEnum()
        {
            return _bEndEnum;
        }

        //public override void Close()
        //{
        //    base.Close();
        //    _sField = null;
        //}
    }
}
