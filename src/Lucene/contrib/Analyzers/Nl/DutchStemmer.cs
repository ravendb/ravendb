/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.IO;
using System.Text;
using System.Collections;
using System.Collections.Generic;

namespace Lucene.Net.Analysis.Nl
{
    /*
     * A stemmer for Dutch words. 
     * <p>
     * The algorithm is an implementation of
     * the <a href="http://snowball.tartarus.org/algorithms/dutch/stemmer.html">dutch stemming</a>
     * algorithm in Martin Porter's snowball project.
     * </p>
     */

    public class DutchStemmer
    {
        /*
         * Buffer for the terms while stemming them.
         */
        private StringBuilder sb = new StringBuilder();
        private bool _removedE;
        private IDictionary<string, string> _stemDict;

        private int _R1;
        private int _R2;

        //TODO convert to internal
        /*
         * Stems the given term to an unique <tt>discriminator</tt>.
         *
         * @param term The term that should be stemmed.
         * @return Discriminator for <tt>term</tt>
         */
        public String Stem(String term)
        {
            term = term.ToLower();
            if (!isStemmable(term))
                return term;
            if (_stemDict != null && _stemDict.ContainsKey(term))
                if (_stemDict[term] is String)
                    return (String)_stemDict[term];
                else
                    return null;

            // Reset the StringBuilder.
            sb.Length = 0;
            sb.Insert(0, term);
            // Stemming starts here...
            substitute(sb);
            storeYandI(sb);
            _R1 = getRIndex(sb, 0);
            _R1 = Math.Max(3, _R1);
            step1(sb);
            step2(sb);
            _R2 = getRIndex(sb, _R1);
            step3a(sb);
            step3b(sb);
            step4(sb);
            reStoreYandI(sb);
            return sb.ToString();
        }

        private bool enEnding(StringBuilder sb)
        {
            String[] enend = new String[] { "ene", "en" };
            for (int i = 0; i < enend.Length; i++)
            {
                String end = enend[i];
                String s = sb.ToString();
                int index = s.Length - end.Length;
                if (s.EndsWith(end) &&
                    index >= _R1 &&
                    isValidEnEnding(sb, index - 1)
                )
                {
                    sb.Remove(index, end.Length);
                    unDouble(sb, index);
                    return true;
                }
            }
            return false;
        }


        private void step1(StringBuilder sb)
        {
            if (_R1 >= sb.Length)
                return;

            String s = sb.ToString();
            int LengthR1 = sb.Length - _R1;
            int index;

            if (s.EndsWith("heden"))
            {
                var toReplace = sb.ToString(_R1, LengthR1).Replace("heden", "heid");
                sb.Remove(_R1, LengthR1);
                sb.Insert(_R1, toReplace);
                return;
            }

            if (enEnding(sb))
                return;

            if (s.EndsWith("se") &&
                (index = s.Length - 2) >= _R1 &&
                isValidSEnding(sb, index - 1)
            )
            {
                sb.Remove(index, 2);
                return;
            }
            if (s.EndsWith("s") &&
                (index = s.Length - 1) >= _R1 &&
                isValidSEnding(sb, index - 1))
            {
                sb.Remove(index, 1);
            }
        }

        /*
         * Remove suffix e if in R1 and
         * preceded by a non-vowel, and then undouble the ending
         *
         * @param sb String being stemmed
         */
        private void step2(StringBuilder sb)
        {
            _removedE = false;
            if (_R1 >= sb.Length)
                return;
            String s = sb.ToString();
            int index = s.Length - 1;
            if (index >= _R1 &&
                s.EndsWith("e") &&
                !isVowel(sb[index - 1]))
            {
                sb.Remove(index, 1);
                unDouble(sb);
                _removedE = true;
            }
        }

        /*
         * Remove "heid"
         *
         * @param sb String being stemmed
         */
        private void step3a(StringBuilder sb)
        {
            if (_R2 >= sb.Length)
                return;
            String s = sb.ToString();
            int index = s.Length - 4;
            if (s.EndsWith("heid") && index >= _R2 && sb[index - 1] != 'c')
            {
                sb.Remove(index, 4); //remove heid
                enEnding(sb);
            }
        }

        /*
         * <p>A d-suffix, or derivational suffix, enables a new word,
         * often with a different grammatical category, or with a different
         * sense, to be built from another word. Whether a d-suffix can be
         * attached is discovered not from the rules of grammar, but by
         * referring to a dictionary. So in English, ness can be added to
         * certain adjectives to form corresponding nouns (littleness,
         * kindness, foolishness ...) but not to all adjectives
         * (not for example, to big, cruel, wise ...) d-suffixes can be
         * used to change meaning, often in rather exotic ways.</p>
         * Remove "ing", "end", "ig", "lijk", "baar" and "bar"
         *
         * @param sb String being stemmed
         */
        private void step3b(StringBuilder sb)
        {
            if (_R2 >= sb.Length)
                return;
            String s = sb.ToString();
            int index = 0;

            if ((s.EndsWith("end") || s.EndsWith("ing")) &&
                (index = s.Length - 3) >= _R2)
            {
                sb.Remove(index, 3);
                if (sb[index - 2] == 'i' &&
                    sb[index - 1] == 'g')
                {
                    if (sb[index - 3] != 'e' & index - 2 >= _R2)
                    {
                        index -= 2;
                        sb.Remove(index, 2);
                    }
                }
                else
                {
                    unDouble(sb, index);
                }
                return;
            }
            if (s.EndsWith("ig") &&
                (index = s.Length - 2) >= _R2
            )
            {
                if (sb[index - 1] != 'e')
                    sb.Remove(index, 2);
                return;
            }
            if (s.EndsWith("lijk") &&
                (index = s.Length - 4) >= _R2
            )
            {
                sb.Remove(index, 4);
                step2(sb);
                return;
            }
            if (s.EndsWith("baar") &&
                (index = s.Length - 4) >= _R2
            )
            {
                sb.Remove(index, 4);
                return;
            }
            if (s.EndsWith("bar") &&
                (index = s.Length - 3) >= _R2
            )
            {
                if (_removedE)
                    sb.Remove(index, 3);
                return;
            }
        }

        /*
         * undouble vowel
         * If the words ends CVD, where C is a non-vowel, D is a non-vowel other than I, and V is double a, e, o or u, remove one of the vowels from V (for example, maan -> man, brood -> brod).
         *
         * @param sb String being stemmed
         */
        private void step4(StringBuilder sb)
        {
            if (sb.Length < 4)
                return;
            String end = sb.ToString(sb.Length - 4, 4);
            char c = end[0];
            char v1 = end[1];
            char v2 = end[2];
            char d = end[3];
            if (v1 == v2 &&
                d != 'I' &&
                v1 != 'i' &&
                isVowel(v1) &&
                !isVowel(d) &&
                !isVowel(c))
            {
                sb.Remove(sb.Length - 2, 1);
            }
        }

        /*
         * Checks if a term could be stemmed.
         *
         * @return true if, and only if, the given term consists in letters.
         */
        private bool isStemmable(String term)
        {
            for (int c = 0; c < term.Length; c++)
            {
                if (!char.IsLetter(term[c])) return false;
            }
            return true;
        }

        /*
         * Substitute Ã¤, Ã«, Ã¯, Ã¶, Ã¼, Ã¡ , Ã©, Ã­, Ã³, Ãº
         */
        private void substitute(StringBuilder buffer)
        {
            for (int i = 0; i < buffer.Length; i++)
            {
                switch (buffer[i])
                {
                    case 'ä':
                    case 'á':
                        {
                            buffer[i] = 'a';
                            break;
                        }
                    case 'ë':
                    case 'é':
                        {
                            buffer[i] = 'e';
                            break;
                        }
                    case 'ü':
                    case 'ú':
                        {
                            buffer[i] = 'u';
                            break;
                        }
                    case 'ï':
                    case 'i':
                        {
                            buffer[i] = 'i';
                            break;
                        }
                    case 'ö':
                    case 'ó':
                        {
                            buffer[i] = 'o';
                            break;
                        }
                }
            }
        }

        /*private bool isValidSEnding(StringBuilder sb) {
          return isValidSEnding(sb, sb.Length - 1);
        }*/

        private bool isValidSEnding(StringBuilder sb, int index)
        {
            char c = sb[index];
            if (isVowel(c) || c == 'j')
                return false;
            return true;
        }

        /*private bool isValidEnEnding(StringBuilder sb) {
          return isValidEnEnding(sb, sb.Length - 1);
        }*/

        private bool isValidEnEnding(StringBuilder sb, int index)
        {
            char c = sb[index];
            if (isVowel(c))
                return false;
            if (c < 3)
                return false;
            // ends with "gem"?
            if (c == 'm' && sb[index - 2] == 'g' && sb[index - 1] == 'e')
                return false;
            return true;
        }

        private void unDouble(StringBuilder sb)
        {
            unDouble(sb, sb.Length);
        }

        private void unDouble(StringBuilder sb, int endIndex)
        {
            String s = sb.ToString(0, endIndex);
            if (s.EndsWith("kk") || s.EndsWith("tt") || s.EndsWith("dd") || s.EndsWith("nn") || s.EndsWith("mm") || s.EndsWith("ff"))
            {
                sb.Remove(endIndex - 1, 1);
            }
        }

        private int getRIndex(StringBuilder sb, int start)
        {
            if (start == 0)
                start = 1;
            int i = start;
            for (; i < sb.Length; i++)
            {
                //first non-vowel preceded by a vowel
                if (!isVowel(sb[i]) && isVowel(sb[i - 1]))
                {
                    return i + 1;
                }
            }
            return i + 1;
        }

        private void storeYandI(StringBuilder sb)
        {
            if (sb[0] == 'y')
                sb[0] = 'Y';

            int last = sb.Length - 1;

            for (int i = 1; i < last; i++)
            {
                switch (sb[i])
                {
                    case 'i':
                        {
                            if (isVowel(sb[i - 1]) &&
                                isVowel(sb[i + 1])
                            )
                                sb[i] = 'I';
                            break;
                        }
                    case 'y':
                        {
                            if (isVowel(sb[i - 1]))
                                sb[i] = 'Y';
                            break;
                        }
                }
            }
            if (last > 0 && sb[last] == 'y' && isVowel(sb[last - 1]))
                sb[last] = 'Y';
        }

        private void reStoreYandI(StringBuilder sb)
        {
            String tmp = sb.ToString();
            sb.Length = 0;
            sb.Insert(0, tmp.Replace("I", "i").Replace("Y", "y"));
        }

        private bool isVowel(char c)
        {
            switch (c)
            {
                case 'e':
                case 'a':
                case 'o':
                case 'i':
                case 'u':
                case 'y':
                case 'è':
                    {
                        return true;
                    }
            }
            return false;
        }

        protected internal void SetStemDictionary(IDictionary<string, string> dict)
        {
            _stemDict = dict;
        }
    }
}