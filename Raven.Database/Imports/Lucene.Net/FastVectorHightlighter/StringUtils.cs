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

namespace Lucene.Net.Search.Vectorhighlight
{
    public static class StringUtils 
    {
        /// <summary>
        /// Check if the termToMatch is a match for the term, considering the use of a wildcards.
        /// </summary>
        public static Boolean TermStringMatch(String term, String termToMatch) 
        {
            if (term[0] == '*' || term[0] == '?')
                throw new NotSupportedException("Unable to do matching with wildcard at the beginning");
            
            if (term[term.Length - 1] == '*') 
            { 
                //Wildcard at the end
                if (termToMatch.Length < term.Length - 1) return false;
                for (int i = 0; i < term.Length - 1; i++)
                {
                    if (termToMatch[i] != term[i]) return false;
                }
                return true;
            }
            return term.Equals(termToMatch);
        }

        public static Boolean AnyTermMatch(IList<String> terms, String term)
        {
            for (int i = 0; i < terms.Count; i++)
            {
                if (StringUtils.TermStringMatch(terms[i], term))
                    return true;
            }
            return false;
        }
     }
}
