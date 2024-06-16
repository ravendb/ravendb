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
	public class PorterStemmer : SnowballProgram
	{
		public PorterStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("s", - 1, 3, "", this), new Among("ies", 0, 2, "", this), new Among("sses", 0, 1, "", this), new Among("ss", 0, - 1, "", this)};
			a_1 = new Among[]{new Among("", - 1, 3, "", this), new Among("bb", 0, 2, "", this), new Among("dd", 0, 2, "", this), new Among("ff", 0, 2, "", this), new Among("gg", 0, 2, "", this), new Among("bl", 0, 1, "", this), new Among("mm", 0, 2, "", this), new Among("nn", 0, 2, "", this), new Among("pp", 0, 2, "", this), new Among("rr", 0, 2, "", this), new Among("at", 0, 1, "", this), new Among("tt", 0, 2, "", this), new Among("iz", 0, 1, "", this)};
			a_2 = new Among[]{new Among("ed", - 1, 2, "", this), new Among("eed", 0, 1, "", this), new Among("ing", - 1, 2, "", this)};
			a_3 = new Among[]{new Among("anci", - 1, 3, "", this), new Among("enci", - 1, 2, "", this), new Among("abli", - 1, 4, "", this), new Among("eli", - 1, 6, "", this), new Among("alli", - 1, 9, "", this), new Among("ousli", - 1, 12, "", this), new Among("entli", - 1, 5, "", this), new Among("aliti", - 1, 10, "", this), new Among("biliti", - 1, 14, "", this), new Among("iviti", - 1, 13, "", this), new Among("tional", - 1, 1, "", this), new Among("ational", 10, 8, "", this), new Among("alism", - 1, 10, "", this), new Among("ation", - 1, 8, "", this), new Among("ization", 13, 7, "", this), new Among("izer", - 1, 7, "", this), new Among("ator", - 1, 8, "", this), new Among("iveness", - 1, 13, "", this), new Among("fulness", - 1, 11, "", this), new Among("ousness", - 1, 12, "", this)};
			a_4 = new Among[]{new Among("icate", - 1, 2, "", this), new Among("ative", - 1, 3, "", this), new Among("alize", - 1, 1, "", this), new Among("iciti", - 1, 2, "", this), new Among("ical", - 1, 2, "", this), new Among("ful", - 1, 3, "", this), new Among("ness", - 1, 3, "", this)};
			a_5 = new Among[]{new Among("ic", - 1, 1, "", this), new Among("ance", - 1, 1, "", this), new Among("ence", - 1, 1, "", this), new Among("able", - 1, 1, "", this), new Among("ible", - 1, 1, "", this), new Among("ate", - 1, 1, "", this), new Among("ive", - 1, 1, "", this), new Among("ize", - 1, 1, "", this), new Among("iti", - 1, 1, "", this), new Among("al", - 1, 1, "", this), new Among("ism", - 1, 1, "", this), new Among("ion", - 1, 2, "", this), new Among("er", - 1, 1, "", this), new Among("ous", - 1, 1, "", this), new Among("ant", - 1, 1, "", this), new Among("ent", - 1, 1, "", this), new Among("ment", 15, 1, "", this), new Among("ement", 16, 1, "", this), new Among("ou", - 1, 1, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private Among[] a_5;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1)};
		private static readonly char[] g_v_WXY = new char[]{(char) (1), (char) (17), (char) (65), (char) (208), (char) (1)};
		
		private bool B_Y_found;
		private int I_p2;
		private int I_p1;
		
		protected internal virtual void  copy_from(PorterStemmer other)
		{
			B_Y_found = other.B_Y_found;
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
			base.copy_from(other);
		}
		
		private bool r_shortv()
		{
			// (, line 19
			if (!(out_grouping_b(g_v_WXY, 89, 121)))
			{
				return false;
			}
			if (!(in_grouping_b(g_v, 97, 121)))
			{
				return false;
			}
			if (!(out_grouping_b(g_v, 97, 121)))
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
		
		private bool r_Step_1a()
		{
			int among_var;
			// (, line 24
			// [, line 25
			ket = cursor;
			// substring, line 25
			among_var = find_among_b(a_0, 4);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 25
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 26
					// <-, line 26
					slice_from("ss");
					break;
				
				case 2: 
					// (, line 27
					// <-, line 27
					slice_from("i");
					break;
				
				case 3: 
					// (, line 29
					// delete, line 29
					slice_del();
					break;
				}
			return true;
		}
		
		private bool r_Step_1b()
		{
			int among_var;
			int v_1;
			int v_3;
			int v_4;
			// (, line 33
			// [, line 34
			ket = cursor;
			// substring, line 34
			among_var = find_among_b(a_2, 3);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 34
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 35
					// call R1, line 35
					if (!r_R1())
					{
						return false;
					}
					// <-, line 35
					slice_from("ee");
					break;
				
				case 2: 
					// (, line 37
					// test, line 38
					v_1 = limit - cursor;
					// gopast, line 38
					while (true)
					{
						do 
						{
							if (!(in_grouping_b(g_v, 97, 121)))
							{
								goto lab1_brk;
							}
							goto golab0_brk;
						}
						while (false);

lab1_brk: ;
						
						if (cursor <= limit_backward)
						{
							return false;
						}
						cursor--;
					}

golab0_brk: ;
					
					cursor = limit - v_1;
					// delete, line 38
					slice_del();
					// test, line 39
					v_3 = limit - cursor;
					// substring, line 39
					among_var = find_among_b(a_1, 13);
					if (among_var == 0)
					{
						return false;
					}
					cursor = limit - v_3;
					switch (among_var)
					{
						
						case 0: 
							return false;
						
						case 1: 
							// (, line 41
							// <+, line 41
							{
								int c = cursor;
								insert(cursor, cursor, "e");
								cursor = c;
							}
							break;
						
						case 2: 
							// (, line 44
							// [, line 44
							ket = cursor;
							// next, line 44
							if (cursor <= limit_backward)
							{
								return false;
							}
							cursor--;
							// ], line 44
							bra = cursor;
							// delete, line 44
							slice_del();
							break;
						
						case 3: 
							// (, line 45
							// atmark, line 45
							if (cursor != I_p1)
							{
								return false;
							}
							// test, line 45
							v_4 = limit - cursor;
							// call shortv, line 45
							if (!r_shortv())
							{
								return false;
							}
							cursor = limit - v_4;
							// <+, line 45
							{
								int c = cursor;
								insert(cursor, cursor, "e");
								cursor = c;
							}
							break;
						}
					break;
				}
			return true;
		}
		
		private bool r_Step_1c()
		{
			int v_1;
			// (, line 51
			// [, line 52
			ket = cursor;
			// or, line 52
			do 
			{
				v_1 = limit - cursor;
				do 
				{
					// literal, line 52
					if (!(eq_s_b(1, "y")))
					{
						goto lab2_brk;
					}
					goto lab0_brk;
				}
				while (false);

lab2_brk: ;
				
				cursor = limit - v_1;
				// literal, line 52
				if (!(eq_s_b(1, "Y")))
				{
					return false;
				}
			}
			while (false);

lab0_brk: ;
			
			// ], line 52
			bra = cursor;
			// gopast, line 53
			while (true)
			{
				do 
				{
					if (!(in_grouping_b(g_v, 97, 121)))
					{
						goto lab3_brk;
					}
					goto golab2_brk;
				}
				while (false);

lab3_brk: ;
				
				if (cursor <= limit_backward)
				{
					return false;
				}
				cursor--;
			}

golab2_brk: ;
			
			// <-, line 54
			slice_from("i");
			return true;
		}
		
		private bool r_Step_2()
		{
			int among_var;
			// (, line 57
			// [, line 58
			ket = cursor;
			// substring, line 58
			among_var = find_among_b(a_3, 20);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 58
			bra = cursor;
			// call R1, line 58
			if (!r_R1())
			{
				return false;
			}
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 59
					// <-, line 59
					slice_from("tion");
					break;
				
				case 2: 
					// (, line 60
					// <-, line 60
					slice_from("ence");
					break;
				
				case 3: 
					// (, line 61
					// <-, line 61
					slice_from("ance");
					break;
				
				case 4: 
					// (, line 62
					// <-, line 62
					slice_from("able");
					break;
				
				case 5: 
					// (, line 63
					// <-, line 63
					slice_from("ent");
					break;
				
				case 6: 
					// (, line 64
					// <-, line 64
					slice_from("e");
					break;
				
				case 7: 
					// (, line 66
					// <-, line 66
					slice_from("ize");
					break;
				
				case 8: 
					// (, line 68
					// <-, line 68
					slice_from("ate");
					break;
				
				case 9: 
					// (, line 69
					// <-, line 69
					slice_from("al");
					break;
				
				case 10: 
					// (, line 71
					// <-, line 71
					slice_from("al");
					break;
				
				case 11: 
					// (, line 72
					// <-, line 72
					slice_from("ful");
					break;
				
				case 12: 
					// (, line 74
					// <-, line 74
					slice_from("ous");
					break;
				
				case 13: 
					// (, line 76
					// <-, line 76
					slice_from("ive");
					break;
				
				case 14: 
					// (, line 77
					// <-, line 77
					slice_from("ble");
					break;
				}
			return true;
		}
		
		private bool r_Step_3()
		{
			int among_var;
			// (, line 81
			// [, line 82
			ket = cursor;
			// substring, line 82
			among_var = find_among_b(a_4, 7);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 82
			bra = cursor;
			// call R1, line 82
			if (!r_R1())
			{
				return false;
			}
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 83
					// <-, line 83
					slice_from("al");
					break;
				
				case 2: 
					// (, line 85
					// <-, line 85
					slice_from("ic");
					break;
				
				case 3: 
					// (, line 87
					// delete, line 87
					slice_del();
					break;
				}
			return true;
		}
		
		private bool r_Step_4()
		{
			int among_var;
			int v_1;
			// (, line 91
			// [, line 92
			ket = cursor;
			// substring, line 92
			among_var = find_among_b(a_5, 19);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 92
			bra = cursor;
			// call R2, line 92
			if (!r_R2())
			{
				return false;
			}
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 95
					// delete, line 95
					slice_del();
					break;
				
				case 2: 
					// (, line 96
					// or, line 96
lab2: 
					do 
					{
						v_1 = limit - cursor;
						do 
						{
							// literal, line 96
							if (!(eq_s_b(1, "s")))
							{
								goto lab2_brk;
							}
							goto lab2_brk;
						}
						while (false);

lab2_brk: ;
						
						cursor = limit - v_1;
						// literal, line 96
						if (!(eq_s_b(1, "t")))
						{
							return false;
						}
					}
					while (false);
					// delete, line 96
					slice_del();
					break;
				}
			return true;
		}
		
		private bool r_Step_5a()
		{
			int v_1;
			int v_2;
			// (, line 100
			// [, line 101
			ket = cursor;
			// literal, line 101
			if (!(eq_s_b(1, "e")))
			{
				return false;
			}
			// ], line 101
			bra = cursor;
			// or, line 102
			do 
			{
				v_1 = limit - cursor;
				do 
				{
					// call R2, line 102
					if (!r_R2())
					{
						goto lab1_brk;
					}
					goto lab0_brk;
				}
				while (false);

lab1_brk: ;
				
				cursor = limit - v_1;
				// (, line 102
				// call R1, line 102
				if (!r_R1())
				{
					return false;
				}
				// not, line 102
				{
					v_2 = limit - cursor;
					do 
					{
						// call shortv, line 102
						if (!r_shortv())
						{
							goto lab2_brk;
						}
						return false;
					}
					while (false);

lab2_brk: ;
					
					cursor = limit - v_2;
				}
			}
			while (false);

lab0_brk: ;

			// delete, line 103
			slice_del();
			return true;
		}
		
		private bool r_Step_5b()
		{
			// (, line 106
			// [, line 107
			ket = cursor;
			// literal, line 107
			if (!(eq_s_b(1, "l")))
			{
				return false;
			}
			// ], line 107
			bra = cursor;
			// call R2, line 108
			if (!r_R2())
			{
				return false;
			}
			// literal, line 108
			if (!(eq_s_b(1, "l")))
			{
				return false;
			}
			// delete, line 109
			slice_del();
			return true;
		}
		
		public override bool Stem()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			int v_5;
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
			// (, line 113
			// unset Y_found, line 115
			B_Y_found = false;
			// do, line 116
			v_1 = cursor;
			do 
			{
				// (, line 116
				// [, line 116
				bra = cursor;
				// literal, line 116
				if (!(eq_s(1, "y")))
				{
					goto lab0_brk;
				}
				// ], line 116
				ket = cursor;
				// <-, line 116
				slice_from("Y");
				// set Y_found, line 116
				B_Y_found = true;
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 117
			v_2 = cursor;
			do 
			{
				// repeat, line 117
				while (true)
				{
					v_3 = cursor;
					do 
					{
						// (, line 117
						// goto, line 117
						while (true)
						{
							v_4 = cursor;
							do 
							{
								// (, line 117
								if (!(in_grouping(g_v, 97, 121)))
								{
									goto lab5_brk;
								}
								// [, line 117
								bra = cursor;
								// literal, line 117
								if (!(eq_s(1, "y")))
								{
									goto lab5_brk;
								}
								// ], line 117
								ket = cursor;
								cursor = v_4;
								goto golab4_brk;
							}
							while (false);

lab5_brk: ;
							
							cursor = v_4;
							if (cursor >= limit)
							{
								goto lab3_brk;
							}
							cursor++;
						}

golab4_brk: ;
						
						// <-, line 117
						slice_from("Y");
						// set Y_found, line 117
						B_Y_found = true;
						goto replab2;
					}
					while (false);

lab3_brk: ;
					
					cursor = v_3;
					goto replab2_brk;

replab2: ;
				}

replab2_brk: ;
				
			}
			while (false);

lab1_brk: ;

			cursor = v_2;
			I_p1 = limit;
			I_p2 = limit;
			// do, line 121
			v_5 = cursor;
			do 
			{
				// (, line 121
				// gopast, line 122
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 121)))
						{
							goto lab8_brk;
						}
						goto golab7_brk;
					}
					while (false);

lab8_brk: ;
					
					if (cursor >= limit)
					{
						goto lab6_brk;
					}
					cursor++;
				}

golab7_brk: ;
				
				// gopast, line 122
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 121)))
						{
							goto lab10_brk;
						}
						goto golab9_brk;
					}
					while (false);

lab10_brk: ;
					
					if (cursor >= limit)
					{
						goto lab6_brk;
					}
					cursor++;
				}

golab9_brk: ;
				
				// setmark p1, line 122
				I_p1 = cursor;
				// gopast, line 123
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 121)))
						{
							goto lab12_brk;
						}
						goto golab11_brk;
					}
					while (false);

lab12_brk: ;
					
					if (cursor >= limit)
					{
						goto lab6_brk;
					}
					cursor++;
				}

golab11_brk: ;
				
				// gopast, line 123
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 121)))
						{
							goto lab14_brk;
						}
						goto golab13_brk;
					}
					while (false);

lab14_brk: ;
					
					if (cursor >= limit)
					{
						goto lab6_brk;
					}
					cursor++;
				}

golab13_brk: ;
				
				// setmark p2, line 123
				I_p2 = cursor;
			}
			while (false);

lab6_brk: ;
			
			cursor = v_5;
			// backwards, line 126
			limit_backward = cursor; cursor = limit;
			// (, line 126
			// do, line 127
			v_10 = limit - cursor;
			do 
			{
				// call Step_1a, line 127
				if (!r_Step_1a())
				{
					goto lab15_brk;
				}
			}
			while (false);

lab15_brk: ;
			
			cursor = limit - v_10;
			// do, line 128
			v_11 = limit - cursor;
			do 
			{
				// call Step_1b, line 128
				if (!r_Step_1b())
				{
					goto lab16_brk;
				}
			}
			while (false);

lab16_brk: ;
			
			cursor = limit - v_11;
			// do, line 129
			v_12 = limit - cursor;
			do 
			{
				// call Step_1c, line 129
				if (!r_Step_1c())
				{
					goto lab17_brk;
				}
			}
			while (false);

lab17_brk: ;
			
			cursor = limit - v_12;
			// do, line 130
			v_13 = limit - cursor;
			do 
			{
				// call Step_2, line 130
				if (!r_Step_2())
				{
					goto lab18_brk;
				}
			}
			while (false);

lab18_brk: ;
			
			cursor = limit - v_13;
			// do, line 131
			v_14 = limit - cursor;
			do 
			{
				// call Step_3, line 131
				if (!r_Step_3())
				{
					goto lab19_brk;
				}
			}
			while (false);

lab19_brk: ;
			
			cursor = limit - v_14;
			// do, line 132
			v_15 = limit - cursor;
			do 
			{
				// call Step_4, line 132
				if (!r_Step_4())
				{
					goto lab20_brk;
				}
			}
			while (false);

lab20_brk: ;
			
			cursor = limit - v_15;
			// do, line 133
			v_16 = limit - cursor;
			do 
			{
				// call Step_5a, line 133
				if (!r_Step_5a())
				{
					goto lab21_brk;
				}
			}
			while (false);

lab21_brk: ;
			
			cursor = limit - v_16;
			// do, line 134
			v_17 = limit - cursor;
			do 
			{
				// call Step_5b, line 134
				if (!r_Step_5b())
				{
					goto lab22_brk;
				}
			}
			while (false);

lab22_brk: ;
			
			cursor = limit - v_17;
			cursor = limit_backward; // do, line 137
			v_18 = cursor;
			do 
			{
				// (, line 137
				// Boolean test Y_found, line 137
				if (!(B_Y_found))
				{
					goto lab23_brk;
				}
				// repeat, line 137
				while (true)
				{
					v_19 = cursor;
					do 
					{
						// (, line 137
						// goto, line 137
						while (true)
						{
							v_20 = cursor;
							do 
							{
								// (, line 137
								// [, line 137
								bra = cursor;
								// literal, line 137
								if (!(eq_s(1, "Y")))
								{
									goto lab27_brk;
								}
								// ], line 137
								ket = cursor;
								cursor = v_20;
								goto golab26_brk;
							}
							while (false);

lab27_brk: ;
							
							cursor = v_20;
							if (cursor >= limit)
							{
								goto lab25_brk;
							}
							cursor++;
						}

golab26_brk: ;
						
						// <-, line 137
						slice_from("y");
						goto replab24;
					}
					while (false);

lab25_brk: ;
					
					cursor = v_19;
					goto replab24_brk;

replab24: ;
				}

replab24_brk: ;
				
			}
			while (false);

lab23_brk: ;
			
			cursor = v_18;
			return true;
		}
	}
}
