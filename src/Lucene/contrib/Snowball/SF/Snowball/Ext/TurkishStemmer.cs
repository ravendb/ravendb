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

/*

Copyright (c) 2001, Dr Martin Porter
Copyright (c) 2002, Richard Boulton
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
    * this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
    * notice, this list of conditions and the following disclaimer in the
    * documentation and/or other materials provided with the distribution.
    * Neither the name of the copyright holders nor the names of its contributors
    * may be used to endorse or promote products derived from this software
    * without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */


using System;
using Among = SF.Snowball.Among;
using SnowballProgram = SF.Snowball.SnowballProgram;
namespace SF.Snowball.Ext
{
    	/* Generated class implementing code defined by a snowball script.
	*
	*/
    public class TurkishStemmer : SnowballProgram
    {

        public TurkishStemmer()
        {
            a_0 = new Among[] {
				new Among("m", -1, -1, "", null),
				new Among("n", -1, -1, "", null),
				new Among("miz", -1, -1, "", null),
				new Among("niz", -1, -1, "", null),
				new Among("muz", -1, -1, "", null),
				new Among("nuz", -1, -1, "", null),
				new Among("m\u00FCz", -1, -1, "", null),
				new Among("n\u00FCz", -1, -1, "", null),
				new Among("m\u0131z", -1, -1, "", null),
				new Among("n\u0131z", -1, -1, "", null)
			};

            a_1 = new Among[] {
				new Among("leri", -1, -1, "", null),
				new Among("lar\u0131", -1, -1, "", null)
			};

            a_2 = new Among[] {
				new Among("ni", -1, -1, "", null),
				new Among("nu", -1, -1, "", null),
				new Among("n\u00FC", -1, -1, "", null),
				new Among("n\u0131", -1, -1, "", null)
			};

            a_3 = new Among[] {
				new Among("in", -1, -1, "", null),
				new Among("un", -1, -1, "", null),
				new Among("\u00FCn", -1, -1, "", null),
				new Among("\u0131n", -1, -1, "", null)
			};

            a_4 = new Among[] {
				new Among("a", -1, -1, "", null),
				new Among("e", -1, -1, "", null)
			};

            a_5 = new Among[] {
				new Among("na", -1, -1, "", null),
				new Among("ne", -1, -1, "", null)
			};

            a_6 = new Among[] {
				new Among("da", -1, -1, "", null),
				new Among("ta", -1, -1, "", null),
				new Among("de", -1, -1, "", null),
				new Among("te", -1, -1, "", null)
			};

            a_7 = new Among[] {
				new Among("nda", -1, -1, "", null),
				new Among("nde", -1, -1, "", null)
			};

            a_8 = new Among[] {
				new Among("dan", -1, -1, "", null),
				new Among("tan", -1, -1, "", null),
				new Among("den", -1, -1, "", null),
				new Among("ten", -1, -1, "", null)
			};

            a_9 = new Among[] {
				new Among("ndan", -1, -1, "", null),
				new Among("nden", -1, -1, "", null)
			};

            a_10 = new Among[] {
				new Among("la", -1, -1, "", null),
				new Among("le", -1, -1, "", null)
			};

            a_11 = new Among[] {
				new Among("ca", -1, -1, "", null),
				new Among("ce", -1, -1, "", null)
			};

            a_12 = new Among[] {
				new Among("im", -1, -1, "", null),
				new Among("um", -1, -1, "", null),
				new Among("\u00FCm", -1, -1, "", null),
				new Among("\u0131m", -1, -1, "", null)
			};

            a_13 = new Among[] {
				new Among("sin", -1, -1, "", null),
				new Among("sun", -1, -1, "", null),
				new Among("s\u00FCn", -1, -1, "", null),
				new Among("s\u0131n", -1, -1, "", null)
			};

            a_14 = new Among[] {
				new Among("iz", -1, -1, "", null),
				new Among("uz", -1, -1, "", null),
				new Among("\u00FCz", -1, -1, "", null),
				new Among("\u0131z", -1, -1, "", null)
			};

            a_15 = new Among[] {
				new Among("siniz", -1, -1, "", null),
				new Among("sunuz", -1, -1, "", null),
				new Among("s\u00FCn\u00FCz", -1, -1, "", null),
				new Among("s\u0131n\u0131z", -1, -1, "", null)
			};

            a_16 = new Among[] {
				new Among("lar", -1, -1, "", null),
				new Among("ler", -1, -1, "", null)
			};

            a_17 = new Among[] {
				new Among("niz", -1, -1, "", null),
				new Among("nuz", -1, -1, "", null),
				new Among("n\u00FCz", -1, -1, "", null),
				new Among("n\u0131z", -1, -1, "", null)
			};

            a_18 = new Among[] {
				new Among("dir", -1, -1, "", null),
				new Among("tir", -1, -1, "", null),
				new Among("dur", -1, -1, "", null),
				new Among("tur", -1, -1, "", null),
				new Among("d\u00FCr", -1, -1, "", null),
				new Among("t\u00FCr", -1, -1, "", null),
				new Among("d\u0131r", -1, -1, "", null),
				new Among("t\u0131r", -1, -1, "", null)
			};

            a_19 = new Among[] {
				new Among("cas\u0131na", -1, -1, "", null),
				new Among("cesine", -1, -1, "", null)
			};

            a_20 = new Among[] {
				new Among("di", -1, -1, "", null),
				new Among("ti", -1, -1, "", null),
				new Among("dik", -1, -1, "", null),
				new Among("tik", -1, -1, "", null),
				new Among("duk", -1, -1, "", null),
				new Among("tuk", -1, -1, "", null),
				new Among("d\u00FCk", -1, -1, "", null),
				new Among("t\u00FCk", -1, -1, "", null),
				new Among("d\u0131k", -1, -1, "", null),
				new Among("t\u0131k", -1, -1, "", null),
				new Among("dim", -1, -1, "", null),
				new Among("tim", -1, -1, "", null),
				new Among("dum", -1, -1, "", null),
				new Among("tum", -1, -1, "", null),
				new Among("d\u00FCm", -1, -1, "", null),
				new Among("t\u00FCm", -1, -1, "", null),
				new Among("d\u0131m", -1, -1, "", null),
				new Among("t\u0131m", -1, -1, "", null),
				new Among("din", -1, -1, "", null),
				new Among("tin", -1, -1, "", null),
				new Among("dun", -1, -1, "", null),
				new Among("tun", -1, -1, "", null),
				new Among("d\u00FCn", -1, -1, "", null),
				new Among("t\u00FCn", -1, -1, "", null),
				new Among("d\u0131n", -1, -1, "", null),
				new Among("t\u0131n", -1, -1, "", null),
				new Among("du", -1, -1, "", null),
				new Among("tu", -1, -1, "", null),
				new Among("d\u00FC", -1, -1, "", null),
				new Among("t\u00FC", -1, -1, "", null),
				new Among("d\u0131", -1, -1, "", null),
				new Among("t\u0131", -1, -1, "", null)
			};

            a_21 = new Among[] {
				new Among("sa", -1, -1, "", null),
				new Among("se", -1, -1, "", null),
				new Among("sak", -1, -1, "", null),
				new Among("sek", -1, -1, "", null),
				new Among("sam", -1, -1, "", null),
				new Among("sem", -1, -1, "", null),
				new Among("san", -1, -1, "", null),
				new Among("sen", -1, -1, "", null)
			};

            a_22 = new Among[] {
				new Among("mi\u015F", -1, -1, "", null),
				new Among("mu\u015F", -1, -1, "", null),
				new Among("m\u00FC\u015F", -1, -1, "", null),
				new Among("m\u0131\u015F", -1, -1, "", null)
			};

            a_23 = new Among[] {
				new Among("b", -1, 1, "", null),
				new Among("c", -1, 2, "", null),
				new Among("d", -1, 3, "", null),
				new Among("\u011F", -1, 4, "", null)
			};

        }

        private Among[] a_0;
        private Among[] a_1;
        private Among[] a_2;
        private Among[] a_3;
        private Among[] a_4;
        private Among[] a_5;
        private Among[] a_6;
        private Among[] a_7;
        private Among[] a_8;
        private Among[] a_9;
        private Among[] a_10;
        private Among[] a_11;
        private Among[] a_12;
        private Among[] a_13;
        private Among[] a_14;
        private Among[] a_15;
        private Among[] a_16;
        private Among[] a_17;
        private Among[] a_18;
        private Among[] a_19;
        private Among[] a_20;
        private Among[] a_21;
        private Among[] a_22;
        private Among[] a_23;
        private static readonly char[] g_vowel = new char[] { (char)17, (char)65, (char)16, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)32, (char)8, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)1 };

        private static readonly char[] g_U = new char[] { (char)1, (char)16, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)8, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)1 };

        private static readonly char[] g_vowel1 = new char[] { (char)1, (char)64, (char)16, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)1 };

        private static readonly char[] g_vowel2 = new char[] { (char)17, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)130 };

        private static readonly char[] g_vowel3 = new char[] { (char)1, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)1 };

        private static readonly char[] g_vowel4 = new char[] { (char)17 };

        private static readonly char[] g_vowel5 = new char[] { (char)65 };

        private static readonly char[] g_vowel6 = new char[] { (char)65 };

        private bool B_continue_stemming_noun_suffixes;
        private int I_strlen;

        private void copy_from(TurkishStemmer other)
        {
            B_continue_stemming_noun_suffixes = other.B_continue_stemming_noun_suffixes;
            I_strlen = other.I_strlen;
            base.copy_from(other);
        }

        private bool r_check_vowel_harmony()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            int v_11;
            // (, line 111
            // test, line 112
            v_1 = limit - cursor;
            // (, line 113
            // (, line 114
            // goto, line 114
            while (true)
            {
                v_2 = limit - cursor;
                if (!(in_grouping_b(g_vowel, 97, 305)))
                {
                    goto lab1;
                }
                cursor = limit - v_2;
                goto golab0;
            lab1:
                cursor = limit - v_2;
                if (cursor <= limit_backward)
                {
                    return false;
                }
                cursor--;
            }
        golab0:
            // (, line 115
            // or, line 116
            v_3 = limit - cursor;
            // (, line 116
            // literal, line 116
            if (!(eq_s_b(1, "a")))
            {
                goto lab3;
            }
            // goto, line 116
            while (true)
            {
                v_4 = limit - cursor;
                if (!(in_grouping_b(g_vowel1, 97, 305)))
                {
                    goto lab5;
                }
                cursor = limit - v_4;
                goto golab4;
            lab5:
                cursor = limit - v_4;
                if (cursor <= limit_backward)
                {
                    goto lab3;
                }
                cursor--;
            }
        golab4:
            goto lab2;
        lab3:
            cursor = limit - v_3;
            // (, line 117
            // literal, line 117
            if (!(eq_s_b(1, "e")))
            {
                goto lab6;
            }
            // goto, line 117
            while (true)
            {
                v_5 = limit - cursor;
                if (!(in_grouping_b(g_vowel2, 101, 252)))
                {
                    goto lab8;
                }
                cursor = limit - v_5;
                goto golab7;
            lab8:
                cursor = limit - v_5;
                if (cursor <= limit_backward)
                {
                    goto lab6;
                }
                cursor--;
            }
        golab7:
            goto lab2;
        lab6:
            cursor = limit - v_3;
            // (, line 118
            // literal, line 118
            if (!(eq_s_b(1, "\u0131")))
            {
                goto lab9;
            }
            // goto, line 118
            while (true)
            {
                v_6 = limit - cursor;
                if (!(in_grouping_b(g_vowel3, 97, 305)))
                {
                    goto lab11;
                }
                cursor = limit - v_6;
                goto golab10;
            lab11:
                cursor = limit - v_6;
                if (cursor <= limit_backward)
                {
                    goto lab9;
                }
                cursor--;
            }
        golab10:
            goto lab2;
        lab9:
            cursor = limit - v_3;
            // (, line 119
            // literal, line 119
            if (!(eq_s_b(1, "i")))
            {
                goto lab12;
            }
            // goto, line 119
            while (true)
            {
                v_7 = limit - cursor;
                if (!(in_grouping_b(g_vowel4, 101, 105)))
                {
                    goto lab14;
                }
                cursor = limit - v_7;
                goto golab13;
            lab14:
                cursor = limit - v_7;
                if (cursor <= limit_backward)
                {
                    goto lab12;
                }
                cursor--;
            }
        golab13:
            goto lab2;
        lab12:
            cursor = limit - v_3;
            // (, line 120
            // literal, line 120
            if (!(eq_s_b(1, "o")))
            {
                goto lab15;
            }
            // goto, line 120
            while (true)
            {
                v_8 = limit - cursor;
                if (!(in_grouping_b(g_vowel5, 111, 117)))
                {
                    goto lab17;
                }
                cursor = limit - v_8;
                goto golab16;
            lab17:
                cursor = limit - v_8;
                if (cursor <= limit_backward)
                {
                    goto lab15;
                }
                cursor--;
            }
        golab16:
            goto lab2;
        lab15:
            cursor = limit - v_3;
            // (, line 121
            // literal, line 121
            if (!(eq_s_b(1, "\u00F6")))
            {
                goto lab18;
            }
            // goto, line 121
            while (true)
            {
                v_9 = limit - cursor;
                if (!(in_grouping_b(g_vowel6, 246, 252)))
                {
                    goto lab20;
                }
                cursor = limit - v_9;
                goto golab19;
            lab20:
                cursor = limit - v_9;
                if (cursor <= limit_backward)
                {
                    goto lab18;
                }
                cursor--;
            }
        golab19:
            goto lab2;
        lab18:
            cursor = limit - v_3;
            // (, line 122
            // literal, line 122
            if (!(eq_s_b(1, "u")))
            {
                goto lab21;
            }
            // goto, line 122
            while (true)
            {
                v_10 = limit - cursor;
                if (!(in_grouping_b(g_vowel5, 111, 117)))
                {
                    goto lab23;
                }
                cursor = limit - v_10;
                goto golab22;
            lab23:
                cursor = limit - v_10;
                if (cursor <= limit_backward)
                {
                    goto lab21;
                }
                cursor--;
            }
        golab22:
            goto lab2;
        lab21:
            cursor = limit - v_3;
            // (, line 123
            // literal, line 123
            if (!(eq_s_b(1, "\u00FC")))
            {
                return false;
            }
            // goto, line 123
            while (true)
            {
                v_11 = limit - cursor;
                if (!(in_grouping_b(g_vowel6, 246, 252)))
                {
                    goto lab25;
                }
                cursor = limit - v_11;
                goto golab24;
            lab25:
                cursor = limit - v_11;
                if (cursor <= limit_backward)
                {
                    return false;
                }
                cursor--;
            }
        golab24:
        lab2:
            cursor = limit - v_1;
            return true;
        }

        private bool r_mark_suffix_with_optional_n_consonant()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            // (, line 132
            // or, line 134
            v_1 = limit - cursor;
            // (, line 133
            // (, line 133
            // test, line 133
            v_2 = limit - cursor;
            // literal, line 133
            if (!(eq_s_b(1, "n")))
            {
                goto lab1;
            }
            cursor = limit - v_2;
            // next, line 133
            if (cursor <= limit_backward)
            {
                goto lab1;
            }
            cursor--;
            // (, line 133
            // test, line 133
            v_3 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                goto lab1;
            }
            cursor = limit - v_3;
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 135
            // (, line 135
            // not, line 135
            {
                v_4 = limit - cursor;
                // (, line 135
                // test, line 135
                v_5 = limit - cursor;
                // literal, line 135
                if (!(eq_s_b(1, "n")))
                {
                    goto lab2;
                }
                cursor = limit - v_5;
                return false;
            lab2:
                cursor = limit - v_4;
            }
            // test, line 135
            v_6 = limit - cursor;
            // (, line 135
            // next, line 135
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // (, line 135
            // test, line 135
            v_7 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                return false;
            }
            cursor = limit - v_7;
            cursor = limit - v_6;
        lab0:
            return true;
        }

        private bool r_mark_suffix_with_optional_s_consonant()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            // (, line 143
            // or, line 145
            v_1 = limit - cursor;
            // (, line 144
            // (, line 144
            // test, line 144
            v_2 = limit - cursor;
            // literal, line 144
            if (!(eq_s_b(1, "s")))
            {
                goto lab1;
            }
            cursor = limit - v_2;
            // next, line 144
            if (cursor <= limit_backward)
            {
                goto lab1;
            }
            cursor--;
            // (, line 144
            // test, line 144
            v_3 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                goto lab1;
            }
            cursor = limit - v_3;
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 146
            // (, line 146
            // not, line 146
            {
                v_4 = limit - cursor;
                // (, line 146
                // test, line 146
                v_5 = limit - cursor;
                // literal, line 146
                if (!(eq_s_b(1, "s")))
                {
                    goto lab2;
                }
                cursor = limit - v_5;
                return false;
            lab2:
                cursor = limit - v_4;
            }
            // test, line 146
            v_6 = limit - cursor;
            // (, line 146
            // next, line 146
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // (, line 146
            // test, line 146
            v_7 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                return false;
            }
            cursor = limit - v_7;
            cursor = limit - v_6;
        lab0:
            return true;
        }

        private bool r_mark_suffix_with_optional_y_consonant()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            // (, line 153
            // or, line 155
            v_1 = limit - cursor;
            // (, line 154
            // (, line 154
            // test, line 154
            v_2 = limit - cursor;
            // literal, line 154
            if (!(eq_s_b(1, "y")))
            {
                goto lab1;
            }
            cursor = limit - v_2;
            // next, line 154
            if (cursor <= limit_backward)
            {
                goto lab1;
            }
            cursor--;
            // (, line 154
            // test, line 154
            v_3 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                goto lab1;
            }
            cursor = limit - v_3;
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 156
            // (, line 156
            // not, line 156
            {
                v_4 = limit - cursor;
                // (, line 156
                // test, line 156
                v_5 = limit - cursor;
                // literal, line 156
                if (!(eq_s_b(1, "y")))
                {
                    goto lab2;
                }
                cursor = limit - v_5;
                return false;
            lab2:
                cursor = limit - v_4;
            }
            // test, line 156
            v_6 = limit - cursor;
            // (, line 156
            // next, line 156
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // (, line 156
            // test, line 156
            v_7 = limit - cursor;
            if (!(in_grouping_b(g_vowel, 97, 305)))
            {
                return false;
            }
            cursor = limit - v_7;
            cursor = limit - v_6;
        lab0:
            return true;
        }

        private bool r_mark_suffix_with_optional_U_vowel()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            // (, line 159
            // or, line 161
            v_1 = limit - cursor;
            // (, line 160
            // (, line 160
            // test, line 160
            v_2 = limit - cursor;
            if (!(in_grouping_b(g_U, 105, 305)))
            {
                goto lab1;
            }
            cursor = limit - v_2;
            // next, line 160
            if (cursor <= limit_backward)
            {
                goto lab1;
            }
            cursor--;
            // (, line 160
            // test, line 160
            v_3 = limit - cursor;
            if (!(out_grouping_b(g_vowel, 97, 305)))
            {
                goto lab1;
            }
            cursor = limit - v_3;
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 162
            // (, line 162
            // not, line 162
            {
                v_4 = limit - cursor;
                // (, line 162
                // test, line 162
                v_5 = limit - cursor;
                if (!(in_grouping_b(g_U, 105, 305)))
                {
                    goto lab2;
                }
                cursor = limit - v_5;
                return false;
            lab2:
                cursor = limit - v_4;
            }
            // test, line 162
            v_6 = limit - cursor;
            // (, line 162
            // next, line 162
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // (, line 162
            // test, line 162
            v_7 = limit - cursor;
            if (!(out_grouping_b(g_vowel, 97, 305)))
            {
                return false;
            }
            cursor = limit - v_7;
            cursor = limit - v_6;
        lab0:
            return true;
        }

        private bool r_mark_possessives()
        {
            // (, line 166
            // among, line 167
            if (find_among_b(a_0, 10) == 0)
            {
                return false;
            }
            // (, line 169
            // call mark_suffix_with_optional_U_vowel, line 169
            if (!r_mark_suffix_with_optional_U_vowel())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_sU()
        {
            // (, line 172
            // call check_vowel_harmony, line 173
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            if (!(in_grouping_b(g_U, 105, 305)))
            {
                return false;
            }
            // (, line 175
            // call mark_suffix_with_optional_s_consonant, line 175
            if (!r_mark_suffix_with_optional_s_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_lArI()
        {
            // (, line 178
            // among, line 179
            if (find_among_b(a_1, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yU()
        {
            // (, line 182
            // call check_vowel_harmony, line 183
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            if (!(in_grouping_b(g_U, 105, 305)))
            {
                return false;
            }
            // (, line 185
            // call mark_suffix_with_optional_y_consonant, line 185
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_nU()
        {
            // (, line 188
            // call check_vowel_harmony, line 189
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 190
            if (find_among_b(a_2, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_nUn()
        {
            // (, line 193
            // call check_vowel_harmony, line 194
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 195
            if (find_among_b(a_3, 4) == 0)
            {
                return false;
            }
            // (, line 196
            // call mark_suffix_with_optional_n_consonant, line 196
            if (!r_mark_suffix_with_optional_n_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yA()
        {
            // (, line 199
            // call check_vowel_harmony, line 200
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 201
            if (find_among_b(a_4, 2) == 0)
            {
                return false;
            }
            // (, line 202
            // call mark_suffix_with_optional_y_consonant, line 202
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_nA()
        {
            // (, line 205
            // call check_vowel_harmony, line 206
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 207
            if (find_among_b(a_5, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_DA()
        {
            // (, line 210
            // call check_vowel_harmony, line 211
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 212
            if (find_among_b(a_6, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ndA()
        {
            // (, line 215
            // call check_vowel_harmony, line 216
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 217
            if (find_among_b(a_7, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_DAn()
        {
            // (, line 220
            // call check_vowel_harmony, line 221
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 222
            if (find_among_b(a_8, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ndAn()
        {
            // (, line 225
            // call check_vowel_harmony, line 226
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 227
            if (find_among_b(a_9, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ylA()
        {
            // (, line 230
            // call check_vowel_harmony, line 231
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 232
            if (find_among_b(a_10, 2) == 0)
            {
                return false;
            }
            // (, line 233
            // call mark_suffix_with_optional_y_consonant, line 233
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ki()
        {
            // (, line 236
            // literal, line 237
            if (!(eq_s_b(2, "ki")))
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ncA()
        {
            // (, line 240
            // call check_vowel_harmony, line 241
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 242
            if (find_among_b(a_11, 2) == 0)
            {
                return false;
            }
            // (, line 243
            // call mark_suffix_with_optional_n_consonant, line 243
            if (!r_mark_suffix_with_optional_n_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yUm()
        {
            // (, line 246
            // call check_vowel_harmony, line 247
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 248
            if (find_among_b(a_12, 4) == 0)
            {
                return false;
            }
            // (, line 249
            // call mark_suffix_with_optional_y_consonant, line 249
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_sUn()
        {
            // (, line 252
            // call check_vowel_harmony, line 253
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 254
            if (find_among_b(a_13, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yUz()
        {
            // (, line 257
            // call check_vowel_harmony, line 258
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 259
            if (find_among_b(a_14, 4) == 0)
            {
                return false;
            }
            // (, line 260
            // call mark_suffix_with_optional_y_consonant, line 260
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_sUnUz()
        {
            // (, line 263
            // among, line 264
            if (find_among_b(a_15, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_lAr()
        {
            // (, line 267
            // call check_vowel_harmony, line 268
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 269
            if (find_among_b(a_16, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_nUz()
        {
            // (, line 272
            // call check_vowel_harmony, line 273
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 274
            if (find_among_b(a_17, 4) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_DUr()
        {
            // (, line 277
            // call check_vowel_harmony, line 278
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 279
            if (find_among_b(a_18, 8) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_cAsInA()
        {
            // (, line 282
            // among, line 283
            if (find_among_b(a_19, 2) == 0)
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yDU()
        {
            // (, line 286
            // call check_vowel_harmony, line 287
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 288
            if (find_among_b(a_20, 32) == 0)
            {
                return false;
            }
            // (, line 292
            // call mark_suffix_with_optional_y_consonant, line 292
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ysA()
        {
            // (, line 296
            // among, line 297
            if (find_among_b(a_21, 8) == 0)
            {
                return false;
            }
            // (, line 298
            // call mark_suffix_with_optional_y_consonant, line 298
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_ymUs_()
        {
            // (, line 301
            // call check_vowel_harmony, line 302
            if (!r_check_vowel_harmony())
            {
                return false;
            }
            // among, line 303
            if (find_among_b(a_22, 4) == 0)
            {
                return false;
            }
            // (, line 304
            // call mark_suffix_with_optional_y_consonant, line 304
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_mark_yken()
        {
            // (, line 307
            // literal, line 308
            if (!(eq_s_b(3, "ken")))
            {
                return false;
            }
            // (, line 308
            // call mark_suffix_with_optional_y_consonant, line 308
            if (!r_mark_suffix_with_optional_y_consonant())
            {
                return false;
            }
            return true;
        }

        private bool r_stem_nominal_verb_suffixes()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            // (, line 311
            // [, line 312
            ket = cursor;
            // set continue_stemming_noun_suffixes, line 313
            B_continue_stemming_noun_suffixes = true;
            // or, line 315
            v_1 = limit - cursor;
            // (, line 314
            // or, line 314
            v_2 = limit - cursor;
            // call mark_ymUs_, line 314
            if (!r_mark_ymUs_())
            {
                goto lab3;
            }
            goto lab2;
        lab3:
            cursor = limit - v_2;
            // call mark_yDU, line 314
            if (!r_mark_yDU())
            {
                goto lab4;
            }
            goto lab2;
        lab4:
            cursor = limit - v_2;
            // call mark_ysA, line 314
            if (!r_mark_ysA())
            {
                goto lab5;
            }
            goto lab2;
        lab5:
            cursor = limit - v_2;
            // call mark_yken, line 314
            if (!r_mark_yken())
            {
                goto lab1;
            }
        lab2:
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 316
            // call mark_cAsInA, line 316
            if (!r_mark_cAsInA())
            {
                goto lab6;
            }
            // (, line 316
            // or, line 316
            v_3 = limit - cursor;
            // call mark_sUnUz, line 316
            if (!r_mark_sUnUz())
            {
                goto lab8;
            }
            goto lab7;
        lab8:
            cursor = limit - v_3;
            // call mark_lAr, line 316
            if (!r_mark_lAr())
            {
                goto lab9;
            }
            goto lab7;
        lab9:
            cursor = limit - v_3;
            // call mark_yUm, line 316
            if (!r_mark_yUm())
            {
                goto lab10;
            }
            goto lab7;
        lab10:
            cursor = limit - v_3;
            // call mark_sUn, line 316
            if (!r_mark_sUn())
            {
                goto lab11;
            }
            goto lab7;
        lab11:
            cursor = limit - v_3;
            // call mark_yUz, line 316
            if (!r_mark_yUz())
            {
                goto lab12;
            }
            goto lab7;
        lab12:
            cursor = limit - v_3;
        lab7:
            // call mark_ymUs_, line 316
            if (!r_mark_ymUs_())
            {
                goto lab6;
            }
            goto lab0;
        lab6:
            cursor = limit - v_1;
            // (, line 318
            // call mark_lAr, line 319
            if (!r_mark_lAr())
            {
                goto lab13;
            }
            // ], line 319
            bra = cursor;
            // delete, line 319
            slice_del();
            // try, line 319
            v_4 = limit - cursor;
            // (, line 319
            // [, line 319
            ket = cursor;
            // (, line 319
            // or, line 319
            v_5 = limit - cursor;
            // call mark_DUr, line 319
            if (!r_mark_DUr())
            {
                goto lab16;
            }
            goto lab15;
        lab16:
            cursor = limit - v_5;
            // call mark_yDU, line 319
            if (!r_mark_yDU())
            {
                goto lab17;
            }
            goto lab15;
        lab17:
            cursor = limit - v_5;
            // call mark_ysA, line 319
            if (!r_mark_ysA())
            {
                goto lab18;
            }
            goto lab15;
        lab18:
            cursor = limit - v_5;
            // call mark_ymUs_, line 319
            if (!r_mark_ymUs_())
            {
                cursor = limit - v_4;
                goto lab14;
            }
        lab15:
        lab14:
            // unset continue_stemming_noun_suffixes, line 320
            B_continue_stemming_noun_suffixes = false;
            goto lab0;
        lab13:
            cursor = limit - v_1;
            // (, line 323
            // call mark_nUz, line 323
            if (!r_mark_nUz())
            {
                goto lab19;
            }
            // (, line 323
            // or, line 323
            v_6 = limit - cursor;
            // call mark_yDU, line 323
            if (!r_mark_yDU())
            {
                goto lab21;
            }
            goto lab20;
        lab21:
            cursor = limit - v_6;
            // call mark_ysA, line 323
            if (!r_mark_ysA())
            {
                goto lab19;
            }
        lab20:
            goto lab0;
        lab19:
            cursor = limit - v_1;
            // (, line 325
            // (, line 325
            // or, line 325
            v_7 = limit - cursor;
            // call mark_sUnUz, line 325
            if (!r_mark_sUnUz())
            {
                goto lab24;
            }
            goto lab23;
        lab24:
            cursor = limit - v_7;
            // call mark_yUz, line 325
            if (!r_mark_yUz())
            {
                goto lab25;
            }
            goto lab23;
        lab25:
            cursor = limit - v_7;
            // call mark_sUn, line 325
            if (!r_mark_sUn())
            {
                goto lab26;
            }
            goto lab23;
        lab26:
            cursor = limit - v_7;
            // call mark_yUm, line 325
            if (!r_mark_yUm())
            {
                goto lab22;
            }
        lab23:
            // ], line 325
            bra = cursor;
            // delete, line 325
            slice_del();
            // try, line 325
            v_8 = limit - cursor;
            // (, line 325
            // [, line 325
            ket = cursor;
            // call mark_ymUs_, line 325
            if (!r_mark_ymUs_())
            {
                cursor = limit - v_8;
                goto lab27;
            }
        lab27:
            goto lab0;
        lab22:
            cursor = limit - v_1;
            // (, line 327
            // call mark_DUr, line 327
            if (!r_mark_DUr())
            {
                return false;
            }
            // ], line 327
            bra = cursor;
            // delete, line 327
            slice_del();
            // try, line 327
            v_9 = limit - cursor;
            // (, line 327
            // [, line 327
            ket = cursor;
            // (, line 327
            // or, line 327
            v_10 = limit - cursor;
            // call mark_sUnUz, line 327
            if (!r_mark_sUnUz())
            {
                goto lab30;
            }
            goto lab29;
        lab30:
            cursor = limit - v_10;
            // call mark_lAr, line 327
            if (!r_mark_lAr())
            {
                goto lab31;
            }
            goto lab29;
        lab31:
            cursor = limit - v_10;
            // call mark_yUm, line 327
            if (!r_mark_yUm())
            {
                goto lab32;
            }
            goto lab29;
        lab32:
            cursor = limit - v_10;
            // call mark_sUn, line 327
            if (!r_mark_sUn())
            {
                goto lab33;
            }
            goto lab29;
        lab33:
            cursor = limit - v_10;
            // call mark_yUz, line 327
            if (!r_mark_yUz())
            {
                goto lab34;
            }
            goto lab29;
        lab34:
            cursor = limit - v_10;
        lab29:
            // call mark_ymUs_, line 327
            if (!r_mark_ymUs_())
            {
                cursor = limit - v_9;
                goto lab28;
            }
        lab28:
        lab0:
            // ], line 328
            bra = cursor;
            // delete, line 328
            slice_del();
            return true;
        }

        private bool r_stem_suffix_chain_before_ki()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            int v_11;
            // (, line 332
            // [, line 333
            ket = cursor;
            // call mark_ki, line 334
            if (!r_mark_ki())
            {
                return false;
            }
            // (, line 335
            // or, line 342
            v_1 = limit - cursor;
            // (, line 336
            // call mark_DA, line 336
            if (!r_mark_DA())
            {
                goto lab1;
            }
            // ], line 336
            bra = cursor;
            // delete, line 336
            slice_del();
            // try, line 336
            v_2 = limit - cursor;
            // (, line 336
            // [, line 336
            ket = cursor;
            // or, line 338
            v_3 = limit - cursor;
            // (, line 337
            // call mark_lAr, line 337
            if (!r_mark_lAr())
            {
                goto lab4;
            }
            // ], line 337
            bra = cursor;
            // delete, line 337
            slice_del();
            // try, line 337
            v_4 = limit - cursor;
            // (, line 337
            // call stem_suffix_chain_before_ki, line 337
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_4;
                goto lab5;
            }
        lab5:
            goto lab3;
        lab4:
            cursor = limit - v_3;
            // (, line 339
            // call mark_possessives, line 339
            if (!r_mark_possessives())
            {
                cursor = limit - v_2;
                goto lab2;
            }
            // ], line 339
            bra = cursor;
            // delete, line 339
            slice_del();
            // try, line 339
            v_5 = limit - cursor;
            // (, line 339
            // [, line 339
            ket = cursor;
            // call mark_lAr, line 339
            if (!r_mark_lAr())
            {
                cursor = limit - v_5;
                goto lab6;
            }
            // ], line 339
            bra = cursor;
            // delete, line 339
            slice_del();
            // call stem_suffix_chain_before_ki, line 339
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_5;
                goto lab6;
            }
        lab6:
        lab3:
        lab2:
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 343
            // call mark_nUn, line 343
            if (!r_mark_nUn())
            {
                goto lab7;
            }
            // ], line 343
            bra = cursor;
            // delete, line 343
            slice_del();
            // try, line 343
            v_6 = limit - cursor;
            // (, line 343
            // [, line 343
            ket = cursor;
            // or, line 345
            v_7 = limit - cursor;
            // (, line 344
            // call mark_lArI, line 344
            if (!r_mark_lArI())
            {
                goto lab10;
            }
            // ], line 344
            bra = cursor;
            // delete, line 344
            slice_del();
            goto lab9;
        lab10:
            cursor = limit - v_7;
            // (, line 346
            // [, line 346
            ket = cursor;
            // or, line 346
            v_8 = limit - cursor;
            // call mark_possessives, line 346
            if (!r_mark_possessives())
            {
                goto lab13;
            }
            goto lab12;
        lab13:
            cursor = limit - v_8;
            // call mark_sU, line 346
            if (!r_mark_sU())
            {
                goto lab11;
            }
        lab12:
            // ], line 346
            bra = cursor;
            // delete, line 346
            slice_del();
            // try, line 346
            v_9 = limit - cursor;
            // (, line 346
            // [, line 346
            ket = cursor;
            // call mark_lAr, line 346
            if (!r_mark_lAr())
            {
                cursor = limit - v_9;
                goto lab14;
            }
            // ], line 346
            bra = cursor;
            // delete, line 346
            slice_del();
            // call stem_suffix_chain_before_ki, line 346
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_9;
                goto lab14;
            }
        lab14:
            goto lab9;
        lab11:
            cursor = limit - v_7;
            // (, line 348
            // call stem_suffix_chain_before_ki, line 348
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_6;
                goto lab8;
            }
        lab9:
        lab8:
            goto lab0;
        lab7:
            cursor = limit - v_1;
            // (, line 351
            // call mark_ndA, line 351
            if (!r_mark_ndA())
            {
                return false;
            }
            // (, line 351
            // or, line 353
            v_10 = limit - cursor;
            // (, line 352
            // call mark_lArI, line 352
            if (!r_mark_lArI())
            {
                goto lab16;
            }
            // ], line 352
            bra = cursor;
            // delete, line 352
            slice_del();
            goto lab15;
        lab16:
            cursor = limit - v_10;
            // (, line 354
            // (, line 354
            // call mark_sU, line 354
            if (!r_mark_sU())
            {
                goto lab17;
            }
            // ], line 354
            bra = cursor;
            // delete, line 354
            slice_del();
            // try, line 354
            v_11 = limit - cursor;
            // (, line 354
            // [, line 354
            ket = cursor;
            // call mark_lAr, line 354
            if (!r_mark_lAr())
            {
                cursor = limit - v_11;
                goto lab18;
            }
            // ], line 354
            bra = cursor;
            // delete, line 354
            slice_del();
            // call stem_suffix_chain_before_ki, line 354
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_11;
                goto lab18;
            }
        lab18:
            goto lab15;
        lab17:
            cursor = limit - v_10;
            // (, line 356
            // call stem_suffix_chain_before_ki, line 356
            if (!r_stem_suffix_chain_before_ki())
            {
                return false;
            }
        lab15:
        lab0:
            return true;
        }

        private bool r_stem_noun_suffixes()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            int v_11;
            int v_12;
            int v_13;
            int v_14;
            int v_15;
            int v_16;
            int v_17;
            int v_18;
            int v_19;
            int v_20;
            int v_21;
            int v_22;
            int v_23;
            int v_24;
            int v_25;
            int v_26;
            int v_27;
            // (, line 361
            // or, line 363
            v_1 = limit - cursor;
            // (, line 362
            // [, line 362
            ket = cursor;
            // call mark_lAr, line 362
            if (!r_mark_lAr())
            {
                goto lab1;
            }
            // ], line 362
            bra = cursor;
            // delete, line 362
            slice_del();
            // try, line 362
            v_2 = limit - cursor;
            // (, line 362
            // call stem_suffix_chain_before_ki, line 362
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_2;
                goto lab2;
            }
        lab2:
            goto lab0;
        lab1:
            cursor = limit - v_1;
            // (, line 364
            // [, line 364
            ket = cursor;
            // call mark_ncA, line 364
            if (!r_mark_ncA())
            {
                goto lab3;
            }
            // ], line 364
            bra = cursor;
            // delete, line 364
            slice_del();
            // try, line 365
            v_3 = limit - cursor;
            // (, line 365
            // or, line 367
            v_4 = limit - cursor;
            // (, line 366
            // [, line 366
            ket = cursor;
            // call mark_lArI, line 366
            if (!r_mark_lArI())
            {
                goto lab6;
            }
            // ], line 366
            bra = cursor;
            // delete, line 366
            slice_del();
            goto lab5;
        lab6:
            cursor = limit - v_4;
            // (, line 368
            // [, line 368
            ket = cursor;
            // or, line 368
            v_5 = limit - cursor;
            // call mark_possessives, line 368
            if (!r_mark_possessives())
            {
                goto lab9;
            }
            goto lab8;
        lab9:
            cursor = limit - v_5;
            // call mark_sU, line 368
            if (!r_mark_sU())
            {
                goto lab7;
            }
        lab8:
            // ], line 368
            bra = cursor;
            // delete, line 368
            slice_del();
            // try, line 368
            v_6 = limit - cursor;
            // (, line 368
            // [, line 368
            ket = cursor;
            // call mark_lAr, line 368
            if (!r_mark_lAr())
            {
                cursor = limit - v_6;
                goto lab10;
            }
            // ], line 368
            bra = cursor;
            // delete, line 368
            slice_del();
            // call stem_suffix_chain_before_ki, line 368
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_6;
                goto lab10;
            }
        lab10:
            goto lab5;
        lab7:
            cursor = limit - v_4;
            // (, line 370
            // [, line 370
            ket = cursor;
            // call mark_lAr, line 370
            if (!r_mark_lAr())
            {
                cursor = limit - v_3;
                goto lab4;
            }
            // ], line 370
            bra = cursor;
            // delete, line 370
            slice_del();
            // call stem_suffix_chain_before_ki, line 370
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_3;
                goto lab4;
            }
        lab5:
        lab4:
            goto lab0;
        lab3:
            cursor = limit - v_1;
            // (, line 374
            // [, line 374
            ket = cursor;
            // (, line 374
            // or, line 374
            v_7 = limit - cursor;
            // call mark_ndA, line 374
            if (!r_mark_ndA())
            {
                goto lab13;
            }
            goto lab12;
        lab13:
            cursor = limit - v_7;
            // call mark_nA, line 374
            if (!r_mark_nA())
            {
                goto lab11;
            }
        lab12:
            // (, line 375
            // or, line 377
            v_8 = limit - cursor;
            // (, line 376
            // call mark_lArI, line 376
            if (!r_mark_lArI())
            {
                goto lab15;
            }
            // ], line 376
            bra = cursor;
            // delete, line 376
            slice_del();
            goto lab14;
        lab15:
            cursor = limit - v_8;
            // (, line 378
            // call mark_sU, line 378
            if (!r_mark_sU())
            {
                goto lab16;
            }
            // ], line 378
            bra = cursor;
            // delete, line 378
            slice_del();
            // try, line 378
            v_9 = limit - cursor;
            // (, line 378
            // [, line 378
            ket = cursor;
            // call mark_lAr, line 378
            if (!r_mark_lAr())
            {
                cursor = limit - v_9;
                goto lab17;
            }
            // ], line 378
            bra = cursor;
            // delete, line 378
            slice_del();
            // call stem_suffix_chain_before_ki, line 378
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_9;
                goto lab17;
            }
        lab17:
            goto lab14;
        lab16:
            cursor = limit - v_8;
            // (, line 380
            // call stem_suffix_chain_before_ki, line 380
            if (!r_stem_suffix_chain_before_ki())
            {
                goto lab11;
            }
        lab14:
            goto lab0;
        lab11:
            cursor = limit - v_1;
            // (, line 384
            // [, line 384
            ket = cursor;
            // (, line 384
            // or, line 384
            v_10 = limit - cursor;
            // call mark_ndAn, line 384
            if (!r_mark_ndAn())
            {
                goto lab20;
            }
            goto lab19;
        lab20:
            cursor = limit - v_10;
            // call mark_nU, line 384
            if (!r_mark_nU())
            {
                goto lab18;
            }
        lab19:
            // (, line 384
            // or, line 384
            v_11 = limit - cursor;
            // (, line 384
            // call mark_sU, line 384
            if (!r_mark_sU())
            {
                goto lab22;
            }
            // ], line 384
            bra = cursor;
            // delete, line 384
            slice_del();
            // try, line 384
            v_12 = limit - cursor;
            // (, line 384
            // [, line 384
            ket = cursor;
            // call mark_lAr, line 384
            if (!r_mark_lAr())
            {
                cursor = limit - v_12;
                goto lab23;
            }
            // ], line 384
            bra = cursor;
            // delete, line 384
            slice_del();
            // call stem_suffix_chain_before_ki, line 384
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_12;
                goto lab23;
            }
        lab23:
            goto lab21;
        lab22:
            cursor = limit - v_11;
            // (, line 384
            // call mark_lArI, line 384
            if (!r_mark_lArI())
            {
                goto lab18;
            }
        lab21:
            goto lab0;
        lab18:
            cursor = limit - v_1;
            // (, line 386
            // [, line 386
            ket = cursor;
            // call mark_DAn, line 386
            if (!r_mark_DAn())
            {
                goto lab24;
            }
            // ], line 386
            bra = cursor;
            // delete, line 386
            slice_del();
            // try, line 386
            v_13 = limit - cursor;
            // (, line 386
            // [, line 386
            ket = cursor;
            // (, line 387
            // or, line 389
            v_14 = limit - cursor;
            // (, line 388
            // call mark_possessives, line 388
            if (!r_mark_possessives())
            {
                goto lab27;
            }
            // ], line 388
            bra = cursor;
            // delete, line 388
            slice_del();
            // try, line 388
            v_15 = limit - cursor;
            // (, line 388
            // [, line 388
            ket = cursor;
            // call mark_lAr, line 388
            if (!r_mark_lAr())
            {
                cursor = limit - v_15;
                goto lab28;
            }
            // ], line 388
            bra = cursor;
            // delete, line 388
            slice_del();
            // call stem_suffix_chain_before_ki, line 388
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_15;
                goto lab28;
            }
        lab28:
            goto lab26;
        lab27:
            cursor = limit - v_14;
            // (, line 390
            // call mark_lAr, line 390
            if (!r_mark_lAr())
            {
                goto lab29;
            }
            // ], line 390
            bra = cursor;
            // delete, line 390
            slice_del();
            // try, line 390
            v_16 = limit - cursor;
            // (, line 390
            // call stem_suffix_chain_before_ki, line 390
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_16;
                goto lab30;
            }
        lab30:
            goto lab26;
        lab29:
            cursor = limit - v_14;
            // (, line 392
            // call stem_suffix_chain_before_ki, line 392
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_13;
                goto lab25;
            }
        lab26:
        lab25:
            goto lab0;
        lab24:
            cursor = limit - v_1;
            // (, line 396
            // [, line 396
            ket = cursor;
            // or, line 396
            v_17 = limit - cursor;
            // call mark_nUn, line 396
            if (!r_mark_nUn())
            {
                goto lab33;
            }
            goto lab32;
        lab33:
            cursor = limit - v_17;
            // call mark_ylA, line 396
            if (!r_mark_ylA())
            {
                goto lab31;
            }
        lab32:
            // ], line 396
            bra = cursor;
            // delete, line 396
            slice_del();
            // try, line 397
            v_18 = limit - cursor;
            // (, line 397
            // or, line 399
            v_19 = limit - cursor;
            // (, line 398
            // [, line 398
            ket = cursor;
            // call mark_lAr, line 398
            if (!r_mark_lAr())
            {
                goto lab36;
            }
            // ], line 398
            bra = cursor;
            // delete, line 398
            slice_del();
            // call stem_suffix_chain_before_ki, line 398
            if (!r_stem_suffix_chain_before_ki())
            {
                goto lab36;
            }
            goto lab35;
        lab36:
            cursor = limit - v_19;
            // (, line 400
            // [, line 400
            ket = cursor;
            // or, line 400
            v_20 = limit - cursor;
            // call mark_possessives, line 400
            if (!r_mark_possessives())
            {
                goto lab39;
            }
            goto lab38;
        lab39:
            cursor = limit - v_20;
            // call mark_sU, line 400
            if (!r_mark_sU())
            {
                goto lab37;
            }
        lab38:
            // ], line 400
            bra = cursor;
            // delete, line 400
            slice_del();
            // try, line 400
            v_21 = limit - cursor;
            // (, line 400
            // [, line 400
            ket = cursor;
            // call mark_lAr, line 400
            if (!r_mark_lAr())
            {
                cursor = limit - v_21;
                goto lab40;
            }
            // ], line 400
            bra = cursor;
            // delete, line 400
            slice_del();
            // call stem_suffix_chain_before_ki, line 400
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_21;
                goto lab40;
            }
        lab40:
            goto lab35;
        lab37:
            cursor = limit - v_19;
            // call stem_suffix_chain_before_ki, line 402
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_18;
                goto lab34;
            }
        lab35:
        lab34:
            goto lab0;
        lab31:
            cursor = limit - v_1;
            // (, line 406
            // [, line 406
            ket = cursor;
            // call mark_lArI, line 406
            if (!r_mark_lArI())
            {
                goto lab41;
            }
            // ], line 406
            bra = cursor;
            // delete, line 406
            slice_del();
            goto lab0;
        lab41:
            cursor = limit - v_1;
            // (, line 408
            // call stem_suffix_chain_before_ki, line 408
            if (!r_stem_suffix_chain_before_ki())
            {
                goto lab42;
            }
            goto lab0;
        lab42:
            cursor = limit - v_1;
            // (, line 410
            // [, line 410
            ket = cursor;
            // or, line 410
            v_22 = limit - cursor;
            // call mark_DA, line 410
            if (!r_mark_DA())
            {
                goto lab45;
            }
            goto lab44;
        lab45:
            cursor = limit - v_22;
            // call mark_yU, line 410
            if (!r_mark_yU())
            {
                goto lab46;
            }
            goto lab44;
        lab46:
            cursor = limit - v_22;
            // call mark_yA, line 410
            if (!r_mark_yA())
            {
                goto lab43;
            }
        lab44:
            // ], line 410
            bra = cursor;
            // delete, line 410
            slice_del();
            // try, line 410
            v_23 = limit - cursor;
            // (, line 410
            // [, line 410
            ket = cursor;
            // (, line 410
            // or, line 410
            v_24 = limit - cursor;
            // (, line 410
            // call mark_possessives, line 410
            if (!r_mark_possessives())
            {
                goto lab49;
            }
            // ], line 410
            bra = cursor;
            // delete, line 410
            slice_del();
            // try, line 410
            v_25 = limit - cursor;
            // (, line 410
            // [, line 410
            ket = cursor;
            // call mark_lAr, line 410
            if (!r_mark_lAr())
            {
                cursor = limit - v_25;
                goto lab50;
            }
        lab50:
            goto lab48;
        lab49:
            cursor = limit - v_24;
            // call mark_lAr, line 410
            if (!r_mark_lAr())
            {
                cursor = limit - v_23;
                goto lab47;
            }
        lab48:
            // ], line 410
            bra = cursor;
            // delete, line 410
            slice_del();
            // [, line 410
            ket = cursor;
            // call stem_suffix_chain_before_ki, line 410
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_23;
                goto lab47;
            }
        lab47:
            goto lab0;
        lab43:
            cursor = limit - v_1;
            // (, line 412
            // [, line 412
            ket = cursor;
            // or, line 412
            v_26 = limit - cursor;
            // call mark_possessives, line 412
            if (!r_mark_possessives())
            {
                goto lab52;
            }
            goto lab51;
        lab52:
            cursor = limit - v_26;
            // call mark_sU, line 412
            if (!r_mark_sU())
            {
                return false;
            }
        lab51:
            // ], line 412
            bra = cursor;
            // delete, line 412
            slice_del();
            // try, line 412
            v_27 = limit - cursor;
            // (, line 412
            // [, line 412
            ket = cursor;
            // call mark_lAr, line 412
            if (!r_mark_lAr())
            {
                cursor = limit - v_27;
                goto lab53;
            }
            // ], line 412
            bra = cursor;
            // delete, line 412
            slice_del();
            // call stem_suffix_chain_before_ki, line 412
            if (!r_stem_suffix_chain_before_ki())
            {
                cursor = limit - v_27;
                goto lab53;
            }
        lab53:
        lab0:
            return true;
        }

        private bool r_post_process_last_consonants()
        {
            int among_var;
            // (, line 415
            // [, line 416
            ket = cursor;
            // substring, line 416
            among_var = find_among_b(a_23, 4);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 416
            bra = cursor;
            switch (among_var)
            {
                case 0:
                    return false;
                case 1:
                    // (, line 417
                    // <-, line 417
                    slice_from("p");
                    break;
                case 2:
                    // (, line 418
                    // <-, line 418
                    slice_from("\u00E7");
                    break;
                case 3:
                    // (, line 419
                    // <-, line 419
                    slice_from("t");
                    break;
                case 4:
                    // (, line 420
                    // <-, line 420
                    slice_from("k");
                    break;
            }
            return true;
        }

        private bool r_append_U_to_stems_ending_with_d_or_g()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            int v_11;
            int v_12;
            int v_13;
            int v_14;
            int v_15;
            // (, line 430
            // test, line 431
            v_1 = limit - cursor;
            // (, line 431
            // or, line 431
            v_2 = limit - cursor;
            // literal, line 431
            if (!(eq_s_b(1, "d")))
            {
                goto lab1;
            }
            goto lab0;
        lab1:
            cursor = limit - v_2;
            // literal, line 431
            if (!(eq_s_b(1, "g")))
            {
                return false;
            }
        lab0:
            cursor = limit - v_1;
            // or, line 433
            v_3 = limit - cursor;
            // (, line 432
            // test, line 432
            v_4 = limit - cursor;
            // (, line 432
            // (, line 432
            // goto, line 432
            while (true)
            {
                v_5 = limit - cursor;
                if (!(in_grouping_b(g_vowel, 97, 305)))
                {
                    goto lab5;
                }
                cursor = limit - v_5;
                goto golab4;
            lab5:
                cursor = limit - v_5;
                if (cursor <= limit_backward)
                {
                    goto lab3;
                }
                cursor--;
            }
        golab4:
            // or, line 432
            v_6 = limit - cursor;
            // literal, line 432
            if (!(eq_s_b(1, "a")))
            {
                goto lab7;
            }
            goto lab6;
        lab7:
            cursor = limit - v_6;
            // literal, line 432
            if (!(eq_s_b(1, "\u0131")))
            {
                goto lab3;
            }
        lab6:
            cursor = limit - v_4;
            // <+, line 432
            {
                int c = cursor;
                insert(cursor, cursor, "\u0131");
                cursor = c;
            }
            goto lab2;
        lab3:
            cursor = limit - v_3;
            // (, line 434
            // test, line 434
            v_7 = limit - cursor;
            // (, line 434
            // (, line 434
            // goto, line 434
            while (true)
            {
                v_8 = limit - cursor;
                if (!(in_grouping_b(g_vowel, 97, 305)))
                {
                    goto lab10;
                }
                cursor = limit - v_8;
                goto golab9;
            lab10:
                cursor = limit - v_8;
                if (cursor <= limit_backward)
                {
                    goto lab8;
                }
                cursor--;
            }
        golab9:
            // or, line 434
            v_9 = limit - cursor;
            // literal, line 434
            if (!(eq_s_b(1, "e")))
            {
                goto lab12;
            }
            goto lab11;
        lab12:
            cursor = limit - v_9;
            // literal, line 434
            if (!(eq_s_b(1, "i")))
            {
                goto lab8;
            }
        lab11:
            cursor = limit - v_7;
            // <+, line 434
            {
                int c = cursor;
                insert(cursor, cursor, "i");
                cursor = c;
            }
            goto lab2;
        lab8:
            cursor = limit - v_3;
            // (, line 436
            // test, line 436
            v_10 = limit - cursor;
            // (, line 436
            // (, line 436
            // goto, line 436
            while (true)
            {
                v_11 = limit - cursor;
                if (!(in_grouping_b(g_vowel, 97, 305)))
                {
                    goto lab15;
                }
                cursor = limit - v_11;
                goto golab14;
            lab15:
                cursor = limit - v_11;
                if (cursor <= limit_backward)
                {
                    goto lab13;
                }
                cursor--;
            }
        golab14:
            // or, line 436
            v_12 = limit - cursor;
            // literal, line 436
            if (!(eq_s_b(1, "o")))
            {
                goto lab17;
            }
            goto lab16;
        lab17:
            cursor = limit - v_12;
            // literal, line 436
            if (!(eq_s_b(1, "u")))
            {
                goto lab13;
            }
        lab16:
            cursor = limit - v_10;
            // <+, line 436
            {
                int c = cursor;
                insert(cursor, cursor, "u");
                cursor = c;
            }
            goto lab2;
        lab13:
            cursor = limit - v_3;
            // (, line 438
            // test, line 438
            v_13 = limit - cursor;
            // (, line 438
            // (, line 438
            // goto, line 438
            while (true)
            {
                v_14 = limit - cursor;
                if (!(in_grouping_b(g_vowel, 97, 305)))
                {
                    goto lab19;
                }
                cursor = limit - v_14;
                goto golab18;
            lab19:
                cursor = limit - v_14;
                if (cursor <= limit_backward)
                {
                    return false;
                }
                cursor--;
            }
        golab18:
            // or, line 438
            v_15 = limit - cursor;
            // literal, line 438
            if (!(eq_s_b(1, "\u00F6")))
            {
                goto lab21;
            }
            goto lab20;
        lab21:
            cursor = limit - v_15;
            // literal, line 438
            if (!(eq_s_b(1, "\u00FC")))
            {
                return false;
            }
        lab20:
            cursor = limit - v_13;
            // <+, line 438
            {
                int c = cursor;
                insert(cursor, cursor, "\u00FC");
                cursor = c;
            }
        lab2:
            return true;
        }

        private bool r_more_than_one_syllable_word()
        {
            int v_1;
            int v_3;
            // (, line 445
            // test, line 446
            v_1 = cursor;
            // (, line 446
            // atleast, line 446
            {
                int v_2 = 2;
            // atleast, line 446
            replab0:
                v_3 = cursor;
                // (, line 446
                // gopast, line 446
                while (true)
                {
                    if (!(in_grouping(g_vowel, 97, 305)))
                    {
                        goto lab3;
                    }
                    goto golab2;
                lab3:
                    if (cursor >= limit)
                    {
                        goto lab1;
                    }
                    cursor++;
                }
            golab2:
                v_2--;
                goto replab0;
            lab1:
                cursor = v_3;
                if (v_2 > 0)
                {
                    return false;
                }
            }
            cursor = v_1;
            return true;
        }

        private bool r_is_reserved_word()
        {
            int v_1;
            int v_2;
            int v_4;
            // (, line 449
            // or, line 451
            v_1 = cursor;
            // test, line 450
            v_2 = cursor;
            // (, line 450
            // gopast, line 450
            while (true)
            {
                // literal, line 450
                if (!(eq_s(2, "ad")))
                {
                    goto lab3;
                }
                goto golab2;
            lab3:
                if (cursor >= limit)
                {
                    goto lab1;
                }
                cursor++;
            }
        golab2:
            // (, line 450
            I_strlen = 2;
            // (, line 450
            if (!(I_strlen == limit))
            {
                goto lab1;
            }
            cursor = v_2;
            goto lab0;
        lab1:
            cursor = v_1;
            // test, line 452
            v_4 = cursor;
            // (, line 452
            // gopast, line 452
            while (true)
            {
                // literal, line 452
                if (!(eq_s(5, "soyad")))
                {
                    goto lab5;
                }
                goto golab4;
            lab5:
                if (cursor >= limit)
                {
                    return false;
                }
                cursor++;
            }
        golab4:
            // (, line 452
            I_strlen = 5;
            // (, line 452
            if (!(I_strlen == limit))
            {
                return false;
            }
            cursor = v_4;
        lab0:
            return true;
        }

        private bool r_postlude()
        {
            int v_1;
            int v_2;
            int v_3;
            // (, line 455
            // not, line 456
            {
                v_1 = cursor;
                // (, line 456
                // call is_reserved_word, line 456
                if (!r_is_reserved_word())
                {
                    goto lab0;
                }
                return false;
            lab0:
                cursor = v_1;
            }
            // backwards, line 457
            limit_backward = cursor; cursor = limit;
            // (, line 457
            // do, line 458
            v_2 = limit - cursor;
            // call append_U_to_stems_ending_with_d_or_g, line 458
            if (!r_append_U_to_stems_ending_with_d_or_g())
            {
                goto lab1;
            }
        lab1:
            cursor = limit - v_2;
            // do, line 459
            v_3 = limit - cursor;
            // call post_process_last_consonants, line 459
            if (!r_post_process_last_consonants())
            {
                goto lab2;
            }
        lab2:
            cursor = limit - v_3;
            cursor = limit_backward;
            return true;
        }

        public override bool Stem()
        {
            int v_1;
            int v_2;
            // (, line 464
            // (, line 465
            // call more_than_one_syllable_word, line 465
            if (!r_more_than_one_syllable_word())
            {
                return false;
            }
            // (, line 466
            // backwards, line 467
            limit_backward = cursor; cursor = limit;
            // (, line 467
            // do, line 468
            v_1 = limit - cursor;
            // call stem_nominal_verb_suffixes, line 468
            if (!r_stem_nominal_verb_suffixes())
            {
                goto lab0;
            }
        lab0:
            cursor = limit - v_1;
            // Boolean test continue_stemming_noun_suffixes, line 469
            if (!(B_continue_stemming_noun_suffixes))
            {
                return false;
            }
            // do, line 470
            v_2 = limit - cursor;
            // call stem_noun_suffixes, line 470
            if (!r_stem_noun_suffixes())
            {
                goto lab1;
            }
        lab1:
            cursor = limit - v_2;
            cursor = limit_backward;
            // call postlude, line 473
            if (!r_postlude())
            {
                return false;
            }
            return true;
        }
    }
}

