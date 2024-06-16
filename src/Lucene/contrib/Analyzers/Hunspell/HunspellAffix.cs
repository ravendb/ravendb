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
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Lucene.Net.Analysis.Hunspell {
    /// <summary>
    ///   Wrapper class representing a hunspell affix.
    /// </summary>
    [DebuggerDisplay("{Condition}")]
    public class HunspellAffix {
        private String _condition;
        private Regex _conditionPattern;

        /// <summary>
        ///   The append defined for the affix.
        /// </summary>
        public String Append { get; set; }

        /// <summary>
        ///   The flags defined for the affix append.
        /// </summary>
        public Char[] AppendFlags { get; set; }

        /// <summary>
        ///   The condition that must be met before the affix can be applied.
        /// </summary>
        public String Condition {
            get { return _condition; }
        }

        /// <summary>
        ///   The affix flag.
        /// </summary>
        public Char Flag { get; set; }

        /// <summary>
        ///   Whether the affix is defined as cross product.
        /// </summary>
        public Boolean IsCrossProduct { get; set; }

        /// <summary>
        ///   The stripping characters defined for the affix.
        /// </summary>
        public String Strip { get; set; }

        /// <summary>
        ///   Checks whether the String defined by the provided char array, offset 
        ///   and length, meets the condition of this affix.
        /// </summary>
        /// <returns>
        ///   <c>true</c> if the String meets the condition, <c>false</c> otherwise.
        /// </returns>
        public Boolean CheckCondition(String text) {
            if (text == null)
                throw new ArgumentNullException("text");

            return _conditionPattern.IsMatch(text);
        }

        /// <summary>
        ///   Sets the condition that must be met before the affix can be applied.
        /// </summary>
        /// <param name="condition">Condition to be met before affix application.</param>
        /// <param name="pattern">Condition as a regular expression pattern.</param>
        public void SetCondition(String condition, String pattern) {
            if (condition == null) throw new ArgumentNullException("condition");
            if (pattern == null) throw new ArgumentNullException("pattern");

            _condition = condition;
            _conditionPattern = new Regex(pattern);
        }
    }
}