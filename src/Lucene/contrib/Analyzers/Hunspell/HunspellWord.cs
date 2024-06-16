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
using System.Linq;

namespace Lucene.Net.Analysis.Hunspell {
    public class HunspellWord {
        private readonly Char[] _flags;

        /// <summary>
        ///   Creates a new HunspellWord with no associated flags.
        /// </summary>
        public HunspellWord() : this(new Char[0]) {
        }

        /// <summary>
        ///   Constructs a new HunspellWord with the given flags.
        /// </summary>
        /// <param name="flags">Flags to associate with the word.</param>
        public HunspellWord(Char[] flags) {
            if (flags == null) 
                throw new ArgumentNullException("flags");

            _flags = flags;
        }

        /// <summary>
        ///   Checks whether the word has the given flag associated with it.
        /// </summary>
        /// <param name="flag">Flag to check whether it is associated with the word.</param>
        /// <returns><c>true</c> if the flag is associated, <c>false</c> otherwise</returns>
        public Boolean HasFlag(Char flag) {
            return _flags.Contains(flag);
        }
    }
}
