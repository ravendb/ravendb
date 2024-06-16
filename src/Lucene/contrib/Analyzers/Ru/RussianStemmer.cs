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
using System.Text;

namespace Lucene.Net.Analysis.Ru
{
    /*
 * Russian stemming algorithm implementation (see http://snowball.sourceforge.net for detailed description).
 */
    public class RussianStemmer
    {
        // positions of RV, R1 and R2 respectively
        private int RV, R1, R2;

        // letters (currently unused letters are commented out)
        private const char A = '\u0430';
        //private const char B = '\u0431';
        private const char V = '\u0432';
        private const char G = '\u0433';
        //private const char D = '\u0434';
        private const char E = '\u0435';
        //private const char ZH = '\u0436';
        //private const char Z = '\u0437';
        private const char I = '\u0438';
        private const char I_ = '\u0439';
        //private const char K = '\u043A';
        private const char L = '\u043B';
        private const char M = '\u043C';
        private const char N = '\u043D';
        private const char O = '\u043E';
        //private const char P = '\u043F';
        //private const char R = '\u0440';
        private const char S = '\u0441';
        private const char T = '\u0442';
        private const char U = '\u0443';
        //private const char F = '\u0444';
        private const char X = '\u0445';
        //private const char TS = '\u0446';
        //private const char CH = '\u0447';
        private const char SH = '\u0448';
        private const char SHCH = '\u0449';
        //private const char HARD = '\u044A';
        private const char Y = '\u044B';
        private const char SOFT = '\u044C';
        private const char AE = '\u044D';
        private const char IU = '\u044E';
        private const char IA = '\u044F';

        // stem definitions
        private static char[] vowels = { A, E, I, O, U, Y, AE, IU, IA };

        private static char[][] perfectiveGerundEndings1 = {
                                                               new[] {V},
                                                               new[] {V, SH, I},
                                                               new[] {V, SH, I, S, SOFT}
                                                           };

        private static char[][] perfectiveGerund1Predessors = {
                                                                  new[] {A},
                                                                  new[] {IA}
                                                              };

        private static char[][] perfectiveGerundEndings2 = {
                                                               new[] {I, V},
                                                               new[] {Y, V},
                                                               new[] {I, V, SH, I},
                                                               new[] {Y, V, SH, I},
                                                               new[] {I, V, SH, I, S, SOFT},
                                                               new[] {Y, V, SH, I, S, SOFT}
                                                           };

        private static char[][] adjectiveEndings = {
                                                       new[] {E, E},
                                                       new[] {I, E},
                                                       new[] {Y, E},
                                                       new[] {O, E},
                                                       new[] {E, I_},
                                                       new[] {I, I_},
                                                       new[] {Y, I_},
                                                       new[] {O, I_},
                                                       new[] {E, M},
                                                       new[] {I, M},
                                                       new[] {Y, M},
                                                       new[] {O, M},
                                                       new[] {I, X},
                                                       new[] {Y, X},
                                                       new[] {U, IU},
                                                       new[] {IU, IU},
                                                       new[] {A, IA},
                                                       new[] {IA, IA},
                                                       new[] {O, IU},
                                                       new[] {E, IU},
                                                       new[] {I, M, I},
                                                       new[] {Y, M, I},
                                                       new[] {E, G, O},
                                                       new[] {O, G, O},
                                                       new[] {E, M, U},
                                                       new[] {O, M, U}
                                                   };

        private static char[][] participleEndings1 = {
                                                         new[] {SHCH},
                                                         new[] {E, M},
                                                         new[] {N, N},
                                                         new[] {V, SH},
                                                         new[] {IU, SHCH}
                                                     };

        private static char[][] participleEndings2 = {
                                                         new[] {I, V, SH},
                                                         new[] {Y, V, SH},
                                                         new[] {U, IU, SHCH}
                                                     };

        private static char[][] participle1Predessors = {
                                                            new[] {A},
                                                            new[] {IA}
                                                        };

        private static char[][] reflexiveEndings = {
                                                       new[] {S, IA},
                                                       new[] {S, SOFT}
                                                   };

        private static char[][] verbEndings1 = {
                                                   new[] {I_},
                                                   new[] {L},
                                                   new[] {N},
                                                   new[] {L, O},
                                                   new[] {N, O},
                                                   new[] {E, T},
                                                   new[] {IU, T},
                                                   new[] {L, A},
                                                   new[] {N, A},
                                                   new[] {L, I},
                                                   new[] {E, M},
                                                   new[] {N, Y},
                                                   new[] {E, T, E},
                                                   new[] {I_, T, E},
                                                   new[] {T, SOFT},
                                                   new[] {E, SH, SOFT},
                                                   new[] {N, N, O}
                                               };

        private static char[][] verbEndings2 = {
                                                   new[] {IU},
                                                   new[] {U, IU},
                                                   new[] {E, N},
                                                   new[] {E, I_},
                                                   new[] {IA, T},
                                                   new[] {U, I_},
                                                   new[] {I, L},
                                                   new[] {Y, L},
                                                   new[] {I, M},
                                                   new[] {Y, M},
                                                   new[] {I, T},
                                                   new[] {Y, T},
                                                   new[] {I, L, A},
                                                   new[] {Y, L, A},
                                                   new[] {E, N, A},
                                                   new[] {I, T, E},
                                                   new[] {I, L, I},
                                                   new[] {Y, L, I},
                                                   new[] {I, L, O},
                                                   new[] {Y, L, O},
                                                   new[] {E, N, O},
                                                   new[] {U, E, T},
                                                   new[] {U, IU, T},
                                                   new[] {E, N, Y},
                                                   new[] {I, T, SOFT},
                                                   new[] {Y, T, SOFT},
                                                   new[] {I, SH, SOFT},
                                                   new[] {E, I_, T, E},
                                                   new[] {U, I_, T, E}
                                               };

        private static char[][] verb1Predessors = {
                                                      new[] {A},
                                                      new[] {IA}
                                                  };

        private static char[][] nounEndings = {
                                                  new[] {A},
                                                  new[] {U},
                                                  new[] {I_},
                                                  new[] {O},
                                                  new[] {U},
                                                  new[] {E},
                                                  new[] {Y},
                                                  new[] {I},
                                                  new[] {SOFT},
                                                  new[] {IA},
                                                  new[] {E, V},
                                                  new[] {O, V},
                                                  new[] {I, E},
                                                  new[] {SOFT, E},
                                                  new[] {IA, X},
                                                  new[] {I, IU},
                                                  new[] {E, I},
                                                  new[] {I, I},
                                                  new[] {E, I_},
                                                  new[] {O, I_},
                                                  new[] {E, M},
                                                  new[] {A, M},
                                                  new[] {O, M},
                                                  new[] {A, X},
                                                  new[] {SOFT, IU},
                                                  new[] {I, IA},
                                                  new[] {SOFT, IA},
                                                  new[] {I, I_},
                                                  new[] {IA, M},
                                                  new[] {IA, M, I},
                                                  new[] {A, M, I},
                                                  new[] {I, E, I_},
                                                  new[] {I, IA, M},
                                                  new[] {I, E, M},
                                                  new[] {I, IA, X},
                                                  new[] {I, IA, M, I}
                                              };

        private static char[][] superlativeEndings = {
                                                         new[] {E, I_, SH},
                                                         new[] {E, I_, SH, E}
                                                     };

        private static char[][] derivationalEndings = {
                                                          new[] {O, S, T},
                                                          new[] {O, S, T, SOFT}
                                                      };

        /*
         * RussianStemmer constructor comment.
         */
        public RussianStemmer()
        {
        }

        /*
         * Adjectival ending is an adjective ending,
         * optionally preceded by participle ending.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool adjectival(StringBuilder stemmingZone)
        {
            // look for adjective ending in a stemming zone
            if (!findAndRemoveEnding(stemmingZone, adjectiveEndings))
                return false;
            // if adjective ending was found, try for participle ending.
            // variable r is unused, we are just interested in the side effect of
            // findAndRemoveEnding():
            bool r =
                findAndRemoveEnding(stemmingZone, participleEndings1, participle1Predessors)
                ||
                findAndRemoveEnding(stemmingZone, participleEndings2);
            return true;
        }

        /*
         * Derivational endings
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool derivational(StringBuilder stemmingZone)
        {
            int endingLength = findEnding(stemmingZone, derivationalEndings);
            if (endingLength == 0)
                // no derivational ending found
                return false;
            else
            {
                // Ensure that the ending locates in R2
                if (R2 - RV <= stemmingZone.Length - endingLength)
                {
                    stemmingZone.Length = stemmingZone.Length - endingLength;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /*
         * Finds ending among given ending class and returns the length of ending found(0, if not found).
         * Creation date: (17/03/2002 8:18:34 PM)
         */
        private int findEnding(StringBuilder stemmingZone, int startIndex, char[][] theEndingClass)
        {
            bool match = false;
            for (int i = theEndingClass.Length - 1; i >= 0; i--)
            {
                char[] theEnding = theEndingClass[i];
                // check if the ending is bigger than stemming zone
                if (startIndex < theEnding.Length - 1)
                {
                    match = false;
                    continue;
                }
                match = true;
                int stemmingIndex = startIndex;
                for (int j = theEnding.Length - 1; j >= 0; j--)
                {
                    if (stemmingZone[stemmingIndex--] != theEnding[j])
                    {
                        match = false;
                        break;
                    }
                }
                // check if ending was found
                if (match)
                {
                    return theEndingClass[i].Length; // cut ending
                }
            }
            return 0;
        }

        private int findEnding(StringBuilder stemmingZone, char[][] theEndingClass)
        {
            return findEnding(stemmingZone, stemmingZone.Length - 1, theEndingClass);
        }

        /*
         * Finds the ending among the given class of endings and removes it from stemming zone.
         * Creation date: (17/03/2002 8:18:34 PM)
         */
        private bool findAndRemoveEnding(StringBuilder stemmingZone, char[][] theEndingClass)
        {
            int endingLength = findEnding(stemmingZone, theEndingClass);
            if (endingLength == 0)
                // not found
                return false;
            else
            {
                stemmingZone.Length = stemmingZone.Length - endingLength;
                // cut the ending found
                return true;
            }
        }

        /*
         * Finds the ending among the given class of endings, then checks if this ending was
         * preceded by any of given predecessors, and if so, removes it from stemming zone.
         * Creation date: (17/03/2002 8:18:34 PM)
         */
        private bool findAndRemoveEnding(StringBuilder stemmingZone,
            char[][] theEndingClass, char[][] thePredessors)
        {
            int endingLength = findEnding(stemmingZone, theEndingClass);
            if (endingLength == 0)
                // not found
                return false;
            else
            {
                int predessorLength =
                    findEnding(stemmingZone,
                        stemmingZone.Length - endingLength - 1,
                        thePredessors);
                if (predessorLength == 0)
                    return false;
                else
                {
                    stemmingZone.Length = stemmingZone.Length - endingLength;
                    // cut the ending found
                    return true;
                }
            }

        }

        /*
         * Marks positions of RV, R1 and R2 in a given word.
         * Creation date: (16/03/2002 3:40:11 PM)
         */
        private void markPositions(String word)
        {
            RV = 0;
            R1 = 0;
            R2 = 0;
            int i = 0;
            // find RV
            while (word.Length > i && !isVowel(word[i]))
            {
                i++;
            }
            if (word.Length - 1 < ++i)
                return; // RV zone is empty
            RV = i;
            // find R1
            while (word.Length > i && isVowel(word[i]))
            {
                i++;
            }
            if (word.Length - 1 < ++i)
                return; // R1 zone is empty
            R1 = i;
            // find R2
            while (word.Length > i && !isVowel(word[i]))
            {
                i++;
            }
            if (word.Length - 1 < ++i)
                return; // R2 zone is empty
            while (word.Length > i && isVowel(word[i]))
            {
                i++;
            }
            if (word.Length - 1 < ++i)
                return; // R2 zone is empty
            R2 = i;
        }

        /*
         * Checks if character is a vowel..
         * Creation date: (16/03/2002 10:47:03 PM)
         * @return bool
         * @param letter char
         */
        private bool isVowel(char letter)
        {
            for (int i = 0; i < vowels.Length; i++)
            {
                if (letter == vowels[i])
                    return true;
            }
            return false;
        }

        /*
         * Noun endings.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool noun(StringBuilder stemmingZone)
        {
            return findAndRemoveEnding(stemmingZone, nounEndings);
        }

        /*
         * Perfective gerund endings.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool perfectiveGerund(StringBuilder stemmingZone)
        {
            return findAndRemoveEnding(
                stemmingZone,
                perfectiveGerundEndings1,
                perfectiveGerund1Predessors)
                || findAndRemoveEnding(stemmingZone, perfectiveGerundEndings2);
        }

        /*
         * Reflexive endings.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool reflexive(StringBuilder stemmingZone)
        {
            return findAndRemoveEnding(stemmingZone, reflexiveEndings);
        }

        /*
         * Insert the method's description here.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool removeI(StringBuilder stemmingZone)
        {
            if (stemmingZone.Length > 0
                && stemmingZone[stemmingZone.Length - 1] == I)
            {
                stemmingZone.Length = stemmingZone.Length - 1;
                return true;
            }
            else
            {
                return false;
            }
        }

        /*
         * Insert the method's description here.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool removeSoft(StringBuilder stemmingZone)
        {
            if (stemmingZone.Length > 0
                && stemmingZone[stemmingZone.Length - 1] == SOFT)
            {
                stemmingZone.Length = stemmingZone.Length - 1;
                return true;
            }
            else
            {
                return false;
            }
        }

        /*
         * Finds the stem for given Russian word.
         * Creation date: (16/03/2002 3:36:48 PM)
         * @return java.lang.String
         * @param input java.lang.String
         */
        public virtual String Stem(String input)
        {
            markPositions(input);
            if (RV == 0)
                return input; //RV wasn't detected, nothing to stem
            StringBuilder stemmingZone = new StringBuilder(input.Substring(RV));
            // stemming goes on in RV
            // Step 1

            if (!perfectiveGerund(stemmingZone))
            {
                reflexive(stemmingZone);
                // variable r is unused, we are just interested in the flow that gets
                // created by logical expression: apply adjectival(); if that fails,
                // apply verb() etc
                bool r =
                    adjectival(stemmingZone)
                    || Verb(stemmingZone)
                    || noun(stemmingZone);
            }
            // Step 2
            removeI(stemmingZone);
            // Step 3
            derivational(stemmingZone);
            // Step 4
            Superlative(stemmingZone);
            UndoubleN(stemmingZone);
            removeSoft(stemmingZone);
            // return result
            return input.Substring(0, RV) + stemmingZone.ToString();
        }

        /*
         * Superlative endings.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool Superlative(StringBuilder stemmingZone)
        {
            return findAndRemoveEnding(stemmingZone, superlativeEndings);
        }

        /*
         * Undoubles N.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool UndoubleN(StringBuilder stemmingZone)
        {
            char[][] doubleN = {
                                   new[] {N, N}
                               };
            if (findEnding(stemmingZone, doubleN) != 0)
            {
                stemmingZone.Length = stemmingZone.Length - 1;
                return true;
            }
            else
            {
                return false;
            }
        }

        /*
         * Verb endings.
         * Creation date: (17/03/2002 12:14:58 AM)
         * @param stemmingZone java.lang.StringBuilder
         */
        private bool Verb(StringBuilder stemmingZone)
        {
            return findAndRemoveEnding(
                stemmingZone,
                verbEndings1,
                verb1Predessors)
                || findAndRemoveEnding(stemmingZone, verbEndings2);
        }

        /*
         * Static method for stemming.
         */
        public static String StemWord(String theWord)
        {
            RussianStemmer stemmer = new RussianStemmer();
            return stemmer.Stem(theWord);
        }
    }
}