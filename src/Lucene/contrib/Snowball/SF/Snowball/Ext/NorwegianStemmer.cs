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

// This file was generated automatically by the Snowball to Java compiler
using System;
using Among = SF.Snowball.Among;
using SnowballProgram = SF.Snowball.SnowballProgram;
namespace SF.Snowball.Ext
{
#pragma warning disable 162,164

	/// <summary> Generated class implementing code defined by a snowball script.</summary>
	public class NorwegianStemmer : SnowballProgram
	{
		public NorwegianStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("a", - 1, 1, "", this), new Among("e", - 1, 1, "", this), new Among("ede", 1, 1, "", this), new Among("ande", 1, 1, "", this), new Among("ende", 1, 1, "", this), new Among("ane", 1, 1, "", this), new Among("ene", 1, 1, "", this), new Among("hetene", 6, 1, "", this), new Among("erte", 1, 3, "", this), new Among("en", - 1, 1, "", this), new Among("heten", 9, 1, "", this), new Among("ar", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("heter", 12, 1, "", this), new Among("s", - 1, 2, "", this), new Among("as", 14, 1, "", this), new Among("es", 14, 1, "", this), new Among("edes", 16, 1, "", this), new Among("endes", 16, 1, "", this), new Among("enes", 16, 1, "", this), new Among("hetenes", 19, 1, "", this), new Among("ens", 14, 1, "", this), new Among("hetens", 21, 1, "", this), new Among("ers", 14, 1, "", this), new Among("ets", 14, 1, "", this), new Among("et", - 1, 1, "", this), new Among("het", 25, 1, "", this), new Among("ert", - 1, 3, "", this), new Among("ast", - 1, 1, "", this)};
			a_1 = new Among[]{new Among("dt", - 1, - 1, "", this), new Among("vt", - 1, - 1, "", this)};
			a_2 = new Among[]{new Among("leg", - 1, 1, "", this), new Among("eleg", 0, 1, "", this), new Among("ig", - 1, 1, "", this), new Among("eig", 2, 1, "", this), new Among("lig", 2, 1, "", this), new Among("elig", 4, 1, "", this), new Among("els", - 1, 1, "", this), new Among("lov", - 1, 1, "", this), new Among("elov", 7, 1, "", this), new Among("slov", 7, 1, "", this), new Among("hetslov", 9, 1, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (48), (char) (0), (char) (128)};
		private static readonly char[] g_s_ending = new char[]{(char) (119), (char) (127), (char) (149), (char) (1)};
		
		private int I_p1;
		
		protected internal virtual void  copy_from(NorwegianStemmer other)
		{
			I_p1 = other.I_p1;
			base.copy_from(other);
		}
		
		private bool r_mark_regions()
		{
			int v_1;
			// (, line 26
			I_p1 = limit;
			// goto, line 30
			while (true)
			{
				v_1 = cursor;
				do 
				{
					if (!(in_grouping(g_v, 97, 248)))
					{
						goto lab1_brk;
					}
					cursor = v_1;
					goto golab0_brk;
				}
				while (false);

lab1_brk: ;
				
				cursor = v_1;
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab0_brk: ;
			
			// gopast, line 30
			while (true)
			{
				do 
				{
					if (!(out_grouping(g_v, 97, 248)))
					{
						goto lab3_brk;
					}
					goto golab2_brk;
				}
				while (false);

lab3_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab2_brk: ;
			
			// setmark p1, line 30
			I_p1 = cursor;
			// try, line 31
			do 
			{
				// (, line 31
				if (!(I_p1 < 3))
				{
					goto lab4_brk;
				}
				I_p1 = 3;
			}
			while (false);

lab4_brk: ;
			
			return true;
		}
		
		private bool r_main_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			// (, line 36
			// setlimit, line 37
			v_1 = limit - cursor;
			// tomark, line 37
			if (cursor < I_p1)
			{
				return false;
			}
			cursor = I_p1;
			v_2 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_1;
			// (, line 37
			// [, line 37
			ket = cursor;
			// substring, line 37
			among_var = find_among_b(a_0, 29);
			if (among_var == 0)
			{
				limit_backward = v_2;
				return false;
			}
			// ], line 37
			bra = cursor;
			limit_backward = v_2;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 43
					// delete, line 43
					slice_del();
					break;
				
				case 2: 
					// (, line 45
					if (!(in_grouping_b(g_s_ending, 98, 122)))
					{
						return false;
					}
					// delete, line 45
					slice_del();
					break;
				
				case 3: 
					// (, line 47
					// <-, line 47
					slice_from("er");
					break;
				}
			return true;
		}
		
		private bool r_consonant_pair()
		{
			int v_1;
			int v_2;
			int v_3;
			// (, line 51
			// test, line 52
			v_1 = limit - cursor;
			// (, line 52
			// setlimit, line 53
			v_2 = limit - cursor;
			// tomark, line 53
			if (cursor < I_p1)
			{
				return false;
			}
			cursor = I_p1;
			v_3 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_2;
			// (, line 53
			// [, line 53
			ket = cursor;
			// substring, line 53
			if (find_among_b(a_1, 2) == 0)
			{
				limit_backward = v_3;
				return false;
			}
			// ], line 53
			bra = cursor;
			limit_backward = v_3;
			cursor = limit - v_1;
			// next, line 58
			if (cursor <= limit_backward)
			{
				return false;
			}
			cursor--;
			// ], line 58
			bra = cursor;
			// delete, line 58
			slice_del();
			return true;
		}
		
		private bool r_other_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			// (, line 61
			// setlimit, line 62
			v_1 = limit - cursor;
			// tomark, line 62
			if (cursor < I_p1)
			{
				return false;
			}
			cursor = I_p1;
			v_2 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_1;
			// (, line 62
			// [, line 62
			ket = cursor;
			// substring, line 62
			among_var = find_among_b(a_2, 11);
			if (among_var == 0)
			{
				limit_backward = v_2;
				return false;
			}
			// ], line 62
			bra = cursor;
			limit_backward = v_2;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 66
					// delete, line 66
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
			// (, line 71
			// do, line 73
			v_1 = cursor;
			do 
			{
				// call mark_regions, line 73
				if (!r_mark_regions())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;
		    	
			cursor = v_1;
			// backwards, line 74
			limit_backward = cursor; cursor = limit;
			// (, line 74
			// do, line 75
			v_2 = limit - cursor;
			do 
			{
				// call main_suffix, line 75
				if (!r_main_suffix())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = limit - v_2;
			// do, line 76
			v_3 = limit - cursor;
			do 
			{
				// call consonant_pair, line 76
				if (!r_consonant_pair())
				{
					goto lab2_brk;
				}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			// do, line 77
			v_4 = limit - cursor;
			do 
			{
				// call other_suffix, line 77
				if (!r_other_suffix())
				{
					goto lab3_brk;
				}
			}
			while (false);

lab3_brk: ;
			
			cursor = limit - v_4;
			cursor = limit_backward; return true;
		}
	}
}
