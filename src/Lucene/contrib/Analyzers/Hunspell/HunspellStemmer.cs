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

namespace Lucene.Net.Analysis.Hunspell {
    /// <summary>
    ///   HunspellStemmer uses the affix rules declared in the HunspellDictionary to generate one or
    ///   more stems for a word.  It conforms to the algorithm in the original hunspell algorithm,
    ///   including recursive suffix stripping.
    /// </summary>
    /// <author>Chris Male</author>
    public class HunspellStemmer {
        private static Int32 RECURSION_CAP = 2;
        private readonly HunspellDictionary _dictionary;

        /// <summary>
        ///   Constructs a new HunspellStemmer which will use the provided HunspellDictionary
        ///   to create its stems.
        /// </summary>
        /// <param name="dictionary">HunspellDictionary that will be used to create the stems.</param>
        public HunspellStemmer(HunspellDictionary dictionary) {
            if (dictionary == null) throw new ArgumentNullException("dictionary");
            _dictionary = dictionary;
        }

        /// <summary>
        ///   Find the stem(s) of the provided word.
        /// </summary>
        /// <param name="word">Word to find the stems for.</param>
        /// <returns>List of stems for the word.</returns>
        public IEnumerable<HunspellStem> Stem(String word) {
            if (word == null) throw new ArgumentNullException("word");

            var stems = new List<HunspellStem>();
            if (_dictionary.LookupWord(word) != null)
                stems.Add(new HunspellStem(word));

            stems.AddRange(Stem(word, null, 0));
            return stems;
        }

        /// <summary>
        ///   Find the unique stem(s) of the provided word.
        /// </summary>
        /// <param name="word">Word to find the stems for.</param>
        /// <returns>List of stems for the word.</returns>
        public IEnumerable<HunspellStem> UniqueStems(String word) {
            if (word == null) throw new ArgumentNullException("word");

            var stems = new List<HunspellStem>();
            var terms = new CharArraySet(8, false);
            if (_dictionary.LookupWord(word) != null) {
                stems.Add(new HunspellStem(word));
                terms.Add(word);
            }

            var otherStems = Stem(word, null, 0);
            foreach (var s in otherStems) {
                if (!terms.Contains(s.Stem)) {
                    stems.Add(s);
                    terms.Add(s.Stem);
                }
            }

            return stems;
        }

        /// <summary>
        ///   Generates a list of stems for the provided word.
        /// </summary>
        /// <param name="word">Word to generate the stems for.</param>
        /// <param name="flags">Flags from a previous stemming step that need to be cross-checked with any affixes in this recursive step.</param>
        /// <param name="recursionDepth">Level of recursion this stemming step is at.</param>
        /// <returns>List of stems, pr an empty if no stems are found.</returns>
        private IEnumerable<HunspellStem> Stem(String word, Char[] flags, Int32 recursionDepth) {
            if (word == null) throw new ArgumentNullException("word");

            var stems = new List<HunspellStem>();
            var chars = word.ToCharArray();
            var length = word.Length;

            for (var i = 0; i < length; i++) {
                var suffixes = _dictionary.LookupSuffix(chars, i, length - i);
                if (suffixes != null) {
                    foreach (var suffix in suffixes) {
                        if (HasCrossCheckedFlag(suffix.Flag, flags)) {
                            var deAffixedLength = length - suffix.Append.Length;

                            // TODO: can we do this in-place?
                            var strippedWord = new StringBuilder()
                                .Append(word, 0, deAffixedLength)
                                .Append(suffix.Strip)
                                .ToString();

                            var stemList = ApplyAffix(strippedWord, suffix, recursionDepth);
                            foreach (var stem in stemList) {
                                stem.AddSuffix(suffix);
                            }

                            stems.AddRange(stemList);
                        }
                    }
                }
            }

            for (var i = length - 1; i >= 0; i--) {
                var prefixes = _dictionary.LookupPrefix(chars, 0, i);
                if (prefixes != null) {
                    foreach (var prefix in prefixes) {
                        if (HasCrossCheckedFlag(prefix.Flag, flags)) {
                            var deAffixedStart = prefix.Append.Length;
                            var deAffixedLength = length - deAffixedStart;

                            var strippedWord = new StringBuilder()
                                .Append(prefix.Strip)
                                .Append(word, deAffixedStart, deAffixedLength)
                                .ToString();

                            var stemList = ApplyAffix(strippedWord, prefix, recursionDepth);
                            foreach (var stem in stemList) {
                                stem.AddPrefix(prefix);
                            }

                            stems.AddRange(stemList);
                        }
                    }
                }
            }

            return stems;
        }

        /// <summary>
        ///   Applies the affix rule to the given word, producing a list of stems if any are found.
        /// </summary>
        /// <param name="strippedWord">Word the affix has been removed and the strip added.</param>
        /// <param name="affix">HunspellAffix representing the affix rule itself.</param>
        /// <param name="recursionDepth">Level of recursion this stemming step is at.</param>
        /// <returns>List of stems for the word, or an empty list if none are found.</returns>
        public IEnumerable<HunspellStem> ApplyAffix(String strippedWord, HunspellAffix affix, Int32 recursionDepth) {
            if (strippedWord == null) throw new ArgumentNullException("strippedWord");
            if (affix == null) throw new ArgumentNullException("affix");

            if (!affix.CheckCondition(strippedWord)) {
                return new List<HunspellStem>();
            }

            var words = _dictionary.LookupWord(strippedWord);
            if (words == null) {
                return new List<HunspellStem>();
            }

            var stems = new List<HunspellStem>();

            foreach (var hunspellWord in words) {
                if (hunspellWord.HasFlag(affix.Flag)) {
                    if (affix.IsCrossProduct && recursionDepth < RECURSION_CAP) {
                        var recursiveStems = Stem(strippedWord, affix.AppendFlags, ++recursionDepth);
                        if (recursiveStems.Any()) {
                            stems.AddRange(recursiveStems);
                        } else {
                            stems.Add(new HunspellStem(strippedWord));
                        }
                    } else {
                        stems.Add(new HunspellStem(strippedWord));
                    }
                }
            }

            return stems;
        }

        /// <summary>
        ///   Checks if the given flag cross checks with the given array of flags.
        /// </summary>
        /// <param name="flag">Flag to cross check with the array of flags.</param>
        /// <param name="flags">Array of flags to cross check against.  Can be <c>null</c>.</param>
        /// <returns><c>true</c> if the flag is found in the array or the array is <c>null</c>, <c>false</c> otherwise.</returns>
        private static Boolean HasCrossCheckedFlag(Char flag, Char[] flags) {
            return flags == null || Array.BinarySearch(flags, flag) >= 0;
        }
    }
}