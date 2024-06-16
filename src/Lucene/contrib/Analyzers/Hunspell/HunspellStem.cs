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

namespace Lucene.Net.Analysis.Hunspell {
    public class HunspellStem {
        private readonly List<HunspellAffix> _prefixes = new List<HunspellAffix>();
        private readonly List<HunspellAffix> _suffixes = new List<HunspellAffix>();
        private readonly String _stem;

        /// <summary>
        ///   the actual word stem itself.
        /// </summary>
        public String Stem {
            get { return _stem; }
        }

        /// <summary>
        ///   The stem length.
        /// </summary>
        public Int32 StemLength {
            get { return _stem.Length; }
        }

        /// <summary>
        ///   The list of prefixes used to generate the stem.
        /// </summary>
        public IEnumerable<HunspellAffix> Prefixes {
            get { return _prefixes; }
        }

        /// <summary>
        ///   The list of suffixes used to generate the stem.
        /// </summary>
        public IEnumerable<HunspellAffix> Suffixes {
            get { return _suffixes; }
        }

        /// <summary>
        ///   Creates a new Stem wrapping the given word stem.
        /// </summary>
        public HunspellStem(String stem) {
            if (stem == null) throw new ArgumentNullException("stem");

            _stem = stem;
        }

        /// <summary>
        ///   Adds a prefix to the list of prefixes used to generate this stem. Because it is 
        ///   assumed that prefixes are added depth first, the prefix is added to the front of 
        ///   the list.
        /// </summary>
        /// <param name="prefix">Prefix to add to the list of prefixes for this stem.</param>
        public void AddPrefix(HunspellAffix prefix) {
            _prefixes.Insert(0, prefix);
        }

        /// <summary>
        ///   Adds a suffix to the list of suffixes used to generate this stem. Because it
        ///   is assumed that suffixes are added depth first, the suffix is added to the end
        ///   of the list.
        /// </summary>
        /// <param name="suffix">Suffix to add to the list of suffixes for this stem.</param>
        public void AddSuffix(HunspellAffix suffix) {
            _suffixes.Add(suffix);
        }
    }
}