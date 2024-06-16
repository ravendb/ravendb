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
    /*
 * Generated class implementing code defined by a snowball script.
 */
    public class RomanianStemmer : SnowballProgram
    {

        public RomanianStemmer()
        {
            a_0 = new Among[] {
				new Among("", -1, 3, "", null),
				new Among("I", 0, 1, "", null),
				new Among("U", 0, 2, "", null)
			};

            a_1 = new Among[] {
				new Among("ea", -1, 3, "", null),
				new Among("a\u0163ia", -1, 7, "", null),
				new Among("aua", -1, 2, "", null),
				new Among("iua", -1, 4, "", null),
				new Among("a\u0163ie", -1, 7, "", null),
				new Among("ele", -1, 3, "", null),
				new Among("ile", -1, 5, "", null),
				new Among("iile", 6, 4, "", null),
				new Among("iei", -1, 4, "", null),
				new Among("atei", -1, 6, "", null),
				new Among("ii", -1, 4, "", null),
				new Among("ului", -1, 1, "", null),
				new Among("ul", -1, 1, "", null),
				new Among("elor", -1, 3, "", null),
				new Among("ilor", -1, 4, "", null),
				new Among("iilor", 14, 4, "", null)
			};

            a_2 = new Among[] {
				new Among("icala", -1, 4, "", null),
				new Among("iciva", -1, 4, "", null),
				new Among("ativa", -1, 5, "", null),
				new Among("itiva", -1, 6, "", null),
				new Among("icale", -1, 4, "", null),
				new Among("a\u0163iune", -1, 5, "", null),
				new Among("i\u0163iune", -1, 6, "", null),
				new Among("atoare", -1, 5, "", null),
				new Among("itoare", -1, 6, "", null),
				new Among("\u0103toare", -1, 5, "", null),
				new Among("icitate", -1, 4, "", null),
				new Among("abilitate", -1, 1, "", null),
				new Among("ibilitate", -1, 2, "", null),
				new Among("ivitate", -1, 3, "", null),
				new Among("icive", -1, 4, "", null),
				new Among("ative", -1, 5, "", null),
				new Among("itive", -1, 6, "", null),
				new Among("icali", -1, 4, "", null),
				new Among("atori", -1, 5, "", null),
				new Among("icatori", 18, 4, "", null),
				new Among("itori", -1, 6, "", null),
				new Among("\u0103tori", -1, 5, "", null),
				new Among("icitati", -1, 4, "", null),
				new Among("abilitati", -1, 1, "", null),
				new Among("ivitati", -1, 3, "", null),
				new Among("icivi", -1, 4, "", null),
				new Among("ativi", -1, 5, "", null),
				new Among("itivi", -1, 6, "", null),
				new Among("icit\u0103i", -1, 4, "", null),
				new Among("abilit\u0103i", -1, 1, "", null),
				new Among("ivit\u0103i", -1, 3, "", null),
				new Among("icit\u0103\u0163i", -1, 4, "", null),
				new Among("abilit\u0103\u0163i", -1, 1, "", null),
				new Among("ivit\u0103\u0163i", -1, 3, "", null),
				new Among("ical", -1, 4, "", null),
				new Among("ator", -1, 5, "", null),
				new Among("icator", 35, 4, "", null),
				new Among("itor", -1, 6, "", null),
				new Among("\u0103tor", -1, 5, "", null),
				new Among("iciv", -1, 4, "", null),
				new Among("ativ", -1, 5, "", null),
				new Among("itiv", -1, 6, "", null),
				new Among("ical\u0103", -1, 4, "", null),
				new Among("iciv\u0103", -1, 4, "", null),
				new Among("ativ\u0103", -1, 5, "", null),
				new Among("itiv\u0103", -1, 6, "", null)
			};

            a_3 = new Among[] {
				new Among("ica", -1, 1, "", null),
				new Among("abila", -1, 1, "", null),
				new Among("ibila", -1, 1, "", null),
				new Among("oasa", -1, 1, "", null),
				new Among("ata", -1, 1, "", null),
				new Among("ita", -1, 1, "", null),
				new Among("anta", -1, 1, "", null),
				new Among("ista", -1, 3, "", null),
				new Among("uta", -1, 1, "", null),
				new Among("iva", -1, 1, "", null),
				new Among("ic", -1, 1, "", null),
				new Among("ice", -1, 1, "", null),
				new Among("abile", -1, 1, "", null),
				new Among("ibile", -1, 1, "", null),
				new Among("isme", -1, 3, "", null),
				new Among("iune", -1, 2, "", null),
				new Among("oase", -1, 1, "", null),
				new Among("ate", -1, 1, "", null),
				new Among("itate", 17, 1, "", null),
				new Among("ite", -1, 1, "", null),
				new Among("ante", -1, 1, "", null),
				new Among("iste", -1, 3, "", null),
				new Among("ute", -1, 1, "", null),
				new Among("ive", -1, 1, "", null),
				new Among("ici", -1, 1, "", null),
				new Among("abili", -1, 1, "", null),
				new Among("ibili", -1, 1, "", null),
				new Among("iuni", -1, 2, "", null),
				new Among("atori", -1, 1, "", null),
				new Among("osi", -1, 1, "", null),
				new Among("ati", -1, 1, "", null),
				new Among("itati", 30, 1, "", null),
				new Among("iti", -1, 1, "", null),
				new Among("anti", -1, 1, "", null),
				new Among("isti", -1, 3, "", null),
				new Among("uti", -1, 1, "", null),
				new Among("i\u015Fti", -1, 3, "", null),
				new Among("ivi", -1, 1, "", null),
				new Among("it\u0103i", -1, 1, "", null),
				new Among("o\u015Fi", -1, 1, "", null),
				new Among("it\u0103\u0163i", -1, 1, "", null),
				new Among("abil", -1, 1, "", null),
				new Among("ibil", -1, 1, "", null),
				new Among("ism", -1, 3, "", null),
				new Among("ator", -1, 1, "", null),
				new Among("os", -1, 1, "", null),
				new Among("at", -1, 1, "", null),
				new Among("it", -1, 1, "", null),
				new Among("ant", -1, 1, "", null),
				new Among("ist", -1, 3, "", null),
				new Among("ut", -1, 1, "", null),
				new Among("iv", -1, 1, "", null),
				new Among("ic\u0103", -1, 1, "", null),
				new Among("abil\u0103", -1, 1, "", null),
				new Among("ibil\u0103", -1, 1, "", null),
				new Among("oas\u0103", -1, 1, "", null),
				new Among("at\u0103", -1, 1, "", null),
				new Among("it\u0103", -1, 1, "", null),
				new Among("ant\u0103", -1, 1, "", null),
				new Among("ist\u0103", -1, 3, "", null),
				new Among("ut\u0103", -1, 1, "", null),
				new Among("iv\u0103", -1, 1, "", null)
			};

            a_4 = new Among[] {
				new Among("ea", -1, 1, "", null),
				new Among("ia", -1, 1, "", null),
				new Among("esc", -1, 1, "", null),
				new Among("\u0103sc", -1, 1, "", null),
				new Among("ind", -1, 1, "", null),
				new Among("\u00E2nd", -1, 1, "", null),
				new Among("are", -1, 1, "", null),
				new Among("ere", -1, 1, "", null),
				new Among("ire", -1, 1, "", null),
				new Among("\u00E2re", -1, 1, "", null),
				new Among("se", -1, 2, "", null),
				new Among("ase", 10, 1, "", null),
				new Among("sese", 10, 2, "", null),
				new Among("ise", 10, 1, "", null),
				new Among("use", 10, 1, "", null),
				new Among("\u00E2se", 10, 1, "", null),
				new Among("e\u015Fte", -1, 1, "", null),
				new Among("\u0103\u015Fte", -1, 1, "", null),
				new Among("eze", -1, 1, "", null),
				new Among("ai", -1, 1, "", null),
				new Among("eai", 19, 1, "", null),
				new Among("iai", 19, 1, "", null),
				new Among("sei", -1, 2, "", null),
				new Among("e\u015Fti", -1, 1, "", null),
				new Among("\u0103\u015Fti", -1, 1, "", null),
				new Among("ui", -1, 1, "", null),
				new Among("ezi", -1, 1, "", null),
				new Among("\u00E2i", -1, 1, "", null),
				new Among("a\u015Fi", -1, 1, "", null),
				new Among("se\u015Fi", -1, 2, "", null),
				new Among("ase\u015Fi", 29, 1, "", null),
				new Among("sese\u015Fi", 29, 2, "", null),
				new Among("ise\u015Fi", 29, 1, "", null),
				new Among("use\u015Fi", 29, 1, "", null),
				new Among("\u00E2se\u015Fi", 29, 1, "", null),
				new Among("i\u015Fi", -1, 1, "", null),
				new Among("u\u015Fi", -1, 1, "", null),
				new Among("\u00E2\u015Fi", -1, 1, "", null),
				new Among("a\u0163i", -1, 2, "", null),
				new Among("ea\u0163i", 38, 1, "", null),
				new Among("ia\u0163i", 38, 1, "", null),
				new Among("e\u0163i", -1, 2, "", null),
				new Among("i\u0163i", -1, 2, "", null),
				new Among("\u00E2\u0163i", -1, 2, "", null),
				new Among("ar\u0103\u0163i", -1, 1, "", null),
				new Among("ser\u0103\u0163i", -1, 2, "", null),
				new Among("aser\u0103\u0163i", 45, 1, "", null),
				new Among("seser\u0103\u0163i", 45, 2, "", null),
				new Among("iser\u0103\u0163i", 45, 1, "", null),
				new Among("user\u0103\u0163i", 45, 1, "", null),
				new Among("\u00E2ser\u0103\u0163i", 45, 1, "", null),
				new Among("ir\u0103\u0163i", -1, 1, "", null),
				new Among("ur\u0103\u0163i", -1, 1, "", null),
				new Among("\u00E2r\u0103\u0163i", -1, 1, "", null),
				new Among("am", -1, 1, "", null),
				new Among("eam", 54, 1, "", null),
				new Among("iam", 54, 1, "", null),
				new Among("em", -1, 2, "", null),
				new Among("asem", 57, 1, "", null),
				new Among("sesem", 57, 2, "", null),
				new Among("isem", 57, 1, "", null),
				new Among("usem", 57, 1, "", null),
				new Among("\u00E2sem", 57, 1, "", null),
				new Among("im", -1, 2, "", null),
				new Among("\u00E2m", -1, 2, "", null),
				new Among("\u0103m", -1, 2, "", null),
				new Among("ar\u0103m", 65, 1, "", null),
				new Among("ser\u0103m", 65, 2, "", null),
				new Among("aser\u0103m", 67, 1, "", null),
				new Among("seser\u0103m", 67, 2, "", null),
				new Among("iser\u0103m", 67, 1, "", null),
				new Among("user\u0103m", 67, 1, "", null),
				new Among("\u00E2ser\u0103m", 67, 1, "", null),
				new Among("ir\u0103m", 65, 1, "", null),
				new Among("ur\u0103m", 65, 1, "", null),
				new Among("\u00E2r\u0103m", 65, 1, "", null),
				new Among("au", -1, 1, "", null),
				new Among("eau", 76, 1, "", null),
				new Among("iau", 76, 1, "", null),
				new Among("indu", -1, 1, "", null),
				new Among("\u00E2ndu", -1, 1, "", null),
				new Among("ez", -1, 1, "", null),
				new Among("easc\u0103", -1, 1, "", null),
				new Among("ar\u0103", -1, 1, "", null),
				new Among("ser\u0103", -1, 2, "", null),
				new Among("aser\u0103", 84, 1, "", null),
				new Among("seser\u0103", 84, 2, "", null),
				new Among("iser\u0103", 84, 1, "", null),
				new Among("user\u0103", 84, 1, "", null),
				new Among("\u00E2ser\u0103", 84, 1, "", null),
				new Among("ir\u0103", -1, 1, "", null),
				new Among("ur\u0103", -1, 1, "", null),
				new Among("\u00E2r\u0103", -1, 1, "", null),
				new Among("eaz\u0103", -1, 1, "", null)
			};

            a_5 = new Among[] {
				new Among("a", -1, 1, "", null),
				new Among("e", -1, 1, "", null),
				new Among("ie", 1, 1, "", null),
				new Among("i", -1, 1, "", null),
				new Among("\u0103", -1, 1, "", null)
			};

        }

        private Among[] a_0;
        private Among[] a_1;
        private Among[] a_2;
        private Among[] a_3;
        private Among[] a_4;
        private Among[] a_5;
        private static readonly char[] g_v = new char[] { (char)17, (char)65, (char)16, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)0, (char)2, (char)32, (char)0, (char)0, (char)4 };

        private bool B_standard_suffix_removed;
        private int I_p2;
        private int I_p1;
        private int I_pV;

        private void copy_from(RomanianStemmer other)
        {
            B_standard_suffix_removed = other.B_standard_suffix_removed;
            I_p2 = other.I_p2;
            I_p1 = other.I_p1;
            I_pV = other.I_pV;
            base.copy_from(other);
        }

        private bool r_prelude()
        {
            int v_1;
            int v_2;
            int v_3;
        // (, line 31
        // repeat, line 32
        replab0:
            v_1 = cursor;
            // goto, line 32
            while (true)
            {
                v_2 = cursor;
                // (, line 32
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab3;
                }
                // [, line 33
                bra = cursor;
                // or, line 33
                v_3 = cursor;
                // (, line 33
                // literal, line 33
                if (!(eq_s(1, "u")))
                {
                    goto lab5;
                }
                // ], line 33
                ket = cursor;
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab5;
                }
                // <-, line 33
                slice_from("U");
                goto lab4;
            lab5:
                cursor = v_3;
                // (, line 34
                // literal, line 34
                if (!(eq_s(1, "i")))
                {
                    goto lab3;
                }
                // ], line 34
                ket = cursor;
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab3;
                }
                // <-, line 34
                slice_from("I");
            lab4:
                cursor = v_2;
                goto golab2;
            lab3:
                cursor = v_2;
                if (cursor >= limit)
                {
                    goto lab1;
                }
                cursor++;
            }
        golab2:
            goto replab0;
        lab1:
            cursor = v_1;
            return true;
        }

        private bool r_mark_regions()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_6;
            int v_8;
            // (, line 38
            I_pV = limit;
            I_p1 = limit;
            I_p2 = limit;
            // do, line 44
            v_1 = cursor;
            // (, line 44
            // or, line 46
            v_2 = cursor;
            // (, line 45
            if (!(in_grouping(g_v, 97, 259)))
            {
                goto lab2;
            }
            // or, line 45
            v_3 = cursor;
            // (, line 45
            if (!(out_grouping(g_v, 97, 259)))
            {
                goto lab4;
            }
            // gopast, line 45
            while (true)
            {
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab6;
                }
                goto golab5;
            lab6:
                if (cursor >= limit)
                {
                    goto lab4;
                }
                cursor++;
            }
        golab5:
            goto lab3;
        lab4:
            cursor = v_3;
            // (, line 45
            if (!(in_grouping(g_v, 97, 259)))
            {
                goto lab2;
            }
            // gopast, line 45
            while (true)
            {
                if (!(out_grouping(g_v, 97, 259)))
                {
                    goto lab8;
                }
                goto golab7;
            lab8:
                if (cursor >= limit)
                {
                    goto lab2;
                }
                cursor++;
            }
        golab7:
        lab3:
            goto lab1;
        lab2:
            cursor = v_2;
            // (, line 47
            if (!(out_grouping(g_v, 97, 259)))
            {
                goto lab0;
            }
            // or, line 47
            v_6 = cursor;
            // (, line 47
            if (!(out_grouping(g_v, 97, 259)))
            {
                goto lab10;
            }
            // gopast, line 47
            while (true)
            {
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab12;
                }
                goto golab11;
            lab12:
                if (cursor >= limit)
                {
                    goto lab10;
                }
                cursor++;
            }
        golab11:
            goto lab9;
        lab10:
            cursor = v_6;
            // (, line 47
            if (!(in_grouping(g_v, 97, 259)))
            {
                goto lab0;
            }
            // next, line 47
            if (cursor >= limit)
            {
                goto lab0;
            }
            cursor++;
        lab9:
        lab1:
            // setmark pV, line 48
            I_pV = cursor;
        lab0:
            cursor = v_1;
            // do, line 50
            v_8 = cursor;
            // (, line 50
            // gopast, line 51
            while (true)
            {
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab15;
                }
                goto golab14;
            lab15:
                if (cursor >= limit)
                {
                    goto lab13;
                }
                cursor++;
            }
        golab14:
            // gopast, line 51
            while (true)
            {
                if (!(out_grouping(g_v, 97, 259)))
                {
                    goto lab17;
                }
                goto golab16;
            lab17:
                if (cursor >= limit)
                {
                    goto lab13;
                }
                cursor++;
            }
        golab16:
            // setmark p1, line 51
            I_p1 = cursor;
            // gopast, line 52
            while (true)
            {
                if (!(in_grouping(g_v, 97, 259)))
                {
                    goto lab19;
                }
                goto golab18;
            lab19:
                if (cursor >= limit)
                {
                    goto lab13;
                }
                cursor++;
            }
        golab18:
            // gopast, line 52
            while (true)
            {
                if (!(out_grouping(g_v, 97, 259)))
                {
                    goto lab21;
                }
                goto golab20;
            lab21:
                if (cursor >= limit)
                {
                    goto lab13;
                }
                cursor++;
            }
        golab20:
            // setmark p2, line 52
            I_p2 = cursor;
        lab13:
            cursor = v_8;
            return true;
        }

        private bool r_postlude()
        {
            int among_var;
            int v_1;
        // repeat, line 56
        replab0:
            v_1 = cursor;
            // (, line 56
            // [, line 58
            bra = cursor;
            // substring, line 58
            among_var = find_among(a_0, 3);
            if (among_var == 0)
            {
                goto lab1;
            }
            // ], line 58
            ket = cursor;
            switch (among_var)
            {
                case 0:
                    goto lab1;
                case 1:
                    // (, line 59
                    // <-, line 59
                    slice_from("i");
                    break;
                case 2:
                    // (, line 60
                    // <-, line 60
                    slice_from("u");
                    break;
                case 3:
                    // (, line 61
                    // next, line 61
                    if (cursor >= limit)
                    {
                        goto lab1;
                    }
                    cursor++;
                    break;
            }
            goto replab0;
        lab1:
            cursor = v_1;
            return true;
        }

        private bool r_RV()
        {
            if (!(I_pV <= cursor))
            {
                return false;
            }
            return true;
        }

        private bool r_R1()
        {
            if (!(I_p1 <= cursor))
            {
                return false;
            }
            return true;
        }

        private bool r_R2()
        {
            if (!(I_p2 <= cursor))
            {
                return false;
            }
            return true;
        }

        private bool r_step_0()
        {
            int among_var;
            int v_1;
            // (, line 72
            // [, line 73
            ket = cursor;
            // substring, line 73
            among_var = find_among_b(a_1, 16);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 73
            bra = cursor;
            // call R1, line 73
            if (!r_R1())
            {
                return false;
            }
            switch (among_var)
            {
                case 0:
                    return false;
                case 1:
                    // (, line 75
                    // delete, line 75
                    slice_del();
                    break;
                case 2:
                    // (, line 77
                    // <-, line 77
                    slice_from("a");
                    break;
                case 3:
                    // (, line 79
                    // <-, line 79
                    slice_from("e");
                    break;
                case 4:
                    // (, line 81
                    // <-, line 81
                    slice_from("i");
                    break;
                case 5:
                    // (, line 83
                    // not, line 83
                    {
                        v_1 = limit - cursor;
                        // literal, line 83
                        if (!(eq_s_b(2, "ab")))
                        {
                            goto lab0;
                        }
                        return false;
                    lab0:
                        cursor = limit - v_1;
                    }
                    // <-, line 83
                    slice_from("i");
                    break;
                case 6:
                    // (, line 85
                    // <-, line 85
                    slice_from("at");
                    break;
                case 7:
                    // (, line 87
                    // <-, line 87
                    slice_from("a\u0163i");
                    break;
            }
            return true;
        }

        private bool r_combo_suffix()
        {
            int among_var;
            int v_1;
            // test, line 91
            v_1 = limit - cursor;
            // (, line 91
            // [, line 92
            ket = cursor;
            // substring, line 92
            among_var = find_among_b(a_2, 46);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 92
            bra = cursor;
            // call R1, line 92
            if (!r_R1())
            {
                return false;
            }
            // (, line 92
            switch (among_var)
            {
                case 0:
                    return false;
                case 1:
                    // (, line 100
                    // <-, line 101
                    slice_from("abil");
                    break;
                case 2:
                    // (, line 103
                    // <-, line 104
                    slice_from("ibil");
                    break;
                case 3:
                    // (, line 106
                    // <-, line 107
                    slice_from("iv");
                    break;
                case 4:
                    // (, line 112
                    // <-, line 113
                    slice_from("ic");
                    break;
                case 5:
                    // (, line 117
                    // <-, line 118
                    slice_from("at");
                    break;
                case 6:
                    // (, line 121
                    // <-, line 122
                    slice_from("it");
                    break;
            }
            // set standard_suffix_removed, line 125
            B_standard_suffix_removed = true;
            cursor = limit - v_1;
            return true;
        }

        private bool r_standard_suffix()
        {
            int among_var;
            int v_1;
            // (, line 129
            // unset standard_suffix_removed, line 130
            B_standard_suffix_removed = false;
        // repeat, line 131
        replab0:
            v_1 = limit - cursor;
            // call combo_suffix, line 131
            if (!r_combo_suffix())
            {
                goto lab1;
            }
            goto replab0;
        lab1:
            cursor = limit - v_1;
            // [, line 132
            ket = cursor;
            // substring, line 132
            among_var = find_among_b(a_3, 62);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 132
            bra = cursor;
            // call R2, line 132
            if (!r_R2())
            {
                return false;
            }
            // (, line 132
            switch (among_var)
            {
                case 0:
                    return false;
                case 1:
                    // (, line 148
                    // delete, line 149
                    slice_del();
                    break;
                case 2:
                    // (, line 151
                    // literal, line 152
                    if (!(eq_s_b(1, "\u0163")))
                    {
                        return false;
                    }
                    // ], line 152
                    bra = cursor;
                    // <-, line 152
                    slice_from("t");
                    break;
                case 3:
                    // (, line 155
                    // <-, line 156
                    slice_from("ist");
                    break;
            }
            // set standard_suffix_removed, line 160
            B_standard_suffix_removed = true;
            return true;
        }

        private bool r_verb_suffix()
        {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            // setlimit, line 164
            v_1 = limit - cursor;
            // tomark, line 164
            if (cursor < I_pV)
            {
                return false;
            }
            cursor = I_pV;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 164
            // [, line 165
            ket = cursor;
            // substring, line 165
            among_var = find_among_b(a_4, 94);
            if (among_var == 0)
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 165
            bra = cursor;
            switch (among_var)
            {
                case 0:
                    limit_backward = v_2;
                    return false;
                case 1:
                    // (, line 200
                    // or, line 200
                    v_3 = limit - cursor;
                    if (!(out_grouping_b(g_v, 97, 259)))
                    {
                        goto lab1;
                    }
                    goto lab0;
                lab1:
                    cursor = limit - v_3;
                    // literal, line 200
                    if (!(eq_s_b(1, "u")))
                    {
                        limit_backward = v_2;
                        return false;
                    }
                lab0:
                    // delete, line 200
                    slice_del();
                    break;
                case 2:
                    // (, line 214
                    // delete, line 214
                    slice_del();
                    break;
            }
            limit_backward = v_2;
            return true;
        }

        private bool r_vowel_suffix()
        {
            int among_var;
            // (, line 218
            // [, line 219
            ket = cursor;
            // substring, line 219
            among_var = find_among_b(a_5, 5);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 219
            bra = cursor;
            // call RV, line 219
            if (!r_RV())
            {
                return false;
            }
            switch (among_var)
            {
                case 0:
                    return false;
                case 1:
                    // (, line 220
                    // delete, line 220
                    slice_del();
                    break;
            }
            return true;
        }

        public override bool Stem()
        {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            // (, line 225
            // do, line 226
            v_1 = cursor;
            // call prelude, line 226
            if (!r_prelude())
            {
                goto lab0;
            }
        lab0:
            cursor = v_1;
            // do, line 227
            v_2 = cursor;
            // call mark_regions, line 227
            if (!r_mark_regions())
            {
                goto lab1;
            }
        lab1:
            cursor = v_2;
            // backwards, line 228
            limit_backward = cursor; cursor = limit;
            // (, line 228
            // do, line 229
            v_3 = limit - cursor;
            // call step_0, line 229
            if (!r_step_0())
            {
                goto lab2;
            }
        lab2:
            cursor = limit - v_3;
            // do, line 230
            v_4 = limit - cursor;
            // call standard_suffix, line 230
            if (!r_standard_suffix())
            {
                goto lab3;
            }
        lab3:
            cursor = limit - v_4;
            // do, line 231
            v_5 = limit - cursor;
            // (, line 231
            // or, line 231
            v_6 = limit - cursor;
            // Boolean test standard_suffix_removed, line 231
            if (!(B_standard_suffix_removed))
            {
                goto lab6;
            }
            goto lab5;
        lab6:
            cursor = limit - v_6;
            // call verb_suffix, line 231
            if (!r_verb_suffix())
            {
                goto lab4;
            }
        lab5:
        lab4:
            cursor = limit - v_5;
            // do, line 232
            v_7 = limit - cursor;
            // call vowel_suffix, line 232
            if (!r_vowel_suffix())
            {
                goto lab7;
            }
        lab7:
            cursor = limit - v_7;
            cursor = limit_backward;
            // do, line 234
            v_8 = cursor;
            // call postlude, line 234
            if (!r_postlude())
            {
                goto lab8;
            }
        lab8:
            cursor = v_8;
            return true;
        }
    }
}
