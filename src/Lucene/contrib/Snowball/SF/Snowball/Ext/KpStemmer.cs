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
	public class KpStemmer : SnowballProgram
	{
		public KpStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("nde", - 1, 7, "", this), new Among("en", - 1, 6, "", this), new Among("s", - 1, 2, "", this), new Among("'s", 2, 1, "", this), new Among("es", 2, 4, "", this), new Among("ies", 4, 3, "", this), new Among("aus", 2, 5, "", this)};
			a_1 = new Among[]{new Among("de", - 1, 5, "", this), new Among("ge", - 1, 2, "", this), new Among("ische", - 1, 4, "", this), new Among("je", - 1, 1, "", this), new Among("lijke", - 1, 3, "", this), new Among("le", - 1, 9, "", this), new Among("ene", - 1, 10, "", this), new Among("re", - 1, 8, "", this), new Among("se", - 1, 7, "", this), new Among("te", - 1, 6, "", this), new Among("ieve", - 1, 11, "", this)};
			a_2 = new Among[]{new Among("heid", - 1, 3, "", this), new Among("fie", - 1, 7, "", this), new Among("gie", - 1, 8, "", this), new Among("atie", - 1, 1, "", this), new Among("isme", - 1, 5, "", this), new Among("ing", - 1, 5, "", this), new Among("arij", - 1, 6, "", this), new Among("erij", - 1, 5, "", this), new Among("sel", - 1, 3, "", this), new Among("rder", - 1, 4, "", this), new Among("ster", - 1, 3, "", this), new Among("iteit", - 1, 2, "", this), new Among("dst", - 1, 10, "", this), new Among("tst", - 1, 9, "", this)};
			a_3 = new Among[]{new Among("end", - 1, 10, "", this), new Among("atief", - 1, 2, "", this), new Among("erig", - 1, 10, "", this), new Among("achtig", - 1, 9, "", this), new Among("ioneel", - 1, 1, "", this), new Among("baar", - 1, 3, "", this), new Among("laar", - 1, 5, "", this), new Among("naar", - 1, 4, "", this), new Among("raar", - 1, 6, "", this), new Among("eriger", - 1, 10, "", this), new Among("achtiger", - 1, 9, "", this), new Among("lijker", - 1, 8, "", this), new Among("tant", - 1, 7, "", this), new Among("erigst", - 1, 10, "", this), new Among("achtigst", - 1, 9, "", this), new Among("lijkst", - 1, 8, "", this)};
			a_4 = new Among[]{new Among("ig", - 1, 1, "", this), new Among("iger", - 1, 1, "", this), new Among("igst", - 1, 1, "", this)};
			a_5 = new Among[]{new Among("ft", - 1, 2, "", this), new Among("kt", - 1, 1, "", this), new Among("pt", - 1, 3, "", this)};
			a_6 = new Among[]{new Among("bb", - 1, 1, "", this), new Among("cc", - 1, 2, "", this), new Among("dd", - 1, 3, "", this), new Among("ff", - 1, 4, "", this), new Among("gg", - 1, 5, "", this), new Among("hh", - 1, 6, "", this), new Among("jj", - 1, 7, "", this), new Among("kk", - 1, 8, "", this), new Among("ll", - 1, 9, "", this), new Among("mm", - 1, 10, "", this), new Among("nn", - 1, 11, "", this), new Among("pp", - 1, 12, "", this), new Among("qq", - 1, 13, "", this), new Among("rr", - 1, 14, "", this), new Among("ss", - 1, 15, "", this), new Among("tt", - 1, 16, "", this), new Among("v", - 1, 21, "", this), new Among("vv", 16, 17, "", this), new Among("ww", - 1, 18, "", this), new Among("xx", - 1, 19, "", this), new Among("z", - 1, 22, "", this), new Among("zz", 20, 20, "", this)};
			a_7 = new Among[]{new Among("d", - 1, 1, "", this), new Among("t", - 1, 2, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private Among[] a_5;
		private Among[] a_6;
		private Among[] a_7;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1)};
		private static readonly char[] g_v_WX = new char[]{(char) (17), (char) (65), (char) (208), (char) (1)};
		private static readonly char[] g_AOU = new char[]{(char) (1), (char) (64), (char) (16)};
		private static readonly char[] g_AIOU = new char[]{(char) (1), (char) (65), (char) (16)};
		
		private bool B_GE_removed;
		private bool B_stemmed;
		private bool B_Y_found;
		private int I_p2;
		private int I_p1;
		private int I_x;
		private System.Text.StringBuilder S_ch = new System.Text.StringBuilder();
		
		protected internal virtual void  copy_from(KpStemmer other)
		{
			B_GE_removed = other.B_GE_removed;
			B_stemmed = other.B_stemmed;
			B_Y_found = other.B_Y_found;
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
			I_x = other.I_x;
			S_ch = other.S_ch;
			base.copy_from(other);
		}
		
		private bool r_R1()
		{
			// (, line 32
			// setmark x, line 32
			I_x = cursor;
			if (!(I_x >= I_p1))
			{
				return false;
			}
			return true;
		}
		
		private bool r_R2()
		{
			// (, line 33
			// setmark x, line 33
			I_x = cursor;
			if (!(I_x >= I_p2))
			{
				return false;
			}
			return true;
		}
		
		private bool r_V()
		{
			int v_1;
			int v_2;
			// test, line 35
			v_1 = limit - cursor;
			// (, line 35
			// or, line 35
			do 
			{
				v_2 = limit - cursor;
				do 
				{
					if (!(in_grouping_b(g_v, 97, 121)))
					{
						goto lab1_brk;
					}
					goto lab0_brk;
				}
				while (false);

lab1_brk: ;
				
				cursor = limit - v_2;
				// literal, line 35
				if (!(eq_s_b(2, "ij")))
				{
					return false;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = limit - v_1;
			return true;
		}
		
		private bool r_VX()
		{
			int v_1;
			int v_2;
			// test, line 36
			v_1 = limit - cursor;
			// (, line 36
			// next, line 36
			if (cursor <= limit_backward)
			{
				return false;
			}
			cursor--;
			// or, line 36
lab2: 
			do 
			{
				v_2 = limit - cursor;
				do 
				{
					if (!(in_grouping_b(g_v, 97, 121)))
					{
						goto lab2_brk;
					}
					goto lab2_brk;
				}
				while (false);

lab2_brk: ;
				
				cursor = limit - v_2;
				// literal, line 36
				if (!(eq_s_b(2, "ij")))
				{
					return false;
				}
			}
			while (false);
			cursor = limit - v_1;
			return true;
		}
		
		private bool r_C()
		{
			int v_1;
			int v_2;
			// test, line 37
			v_1 = limit - cursor;
			// (, line 37
			// not, line 37
			{
				v_2 = limit - cursor;
				do 
				{
					// literal, line 37
					if (!(eq_s_b(2, "ij")))
					{
						goto lab2_brk;
					}
					return false;
				}
				while (false);

lab2_brk: ;
				
				cursor = limit - v_2;
			}
			if (!(out_grouping_b(g_v, 97, 121)))
			{
				return false;
			}
			cursor = limit - v_1;
			return true;
		}
		
		private bool r_lengthen_V()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			int v_5;
			int v_6;
			int v_7;
			int v_8;
			// do, line 39
			v_1 = limit - cursor;
			do 
			{
				// (, line 39
				if (!(out_grouping_b(g_v_WX, 97, 121)))
				{
					goto lab0_brk;
				}
				// [, line 40
				ket = cursor;
				// or, line 40
				do 
				{
					v_2 = limit - cursor;
					do 
					{
						// (, line 40
						if (!(in_grouping_b(g_AOU, 97, 117)))
						{
							goto lab2_brk;
						}
						// ], line 40
						bra = cursor;
						// test, line 40
						v_3 = limit - cursor;
						// (, line 40
						// or, line 40
						do 
						{
							v_4 = limit - cursor;
							do 
							{
								if (!(out_grouping_b(g_v, 97, 121)))
								{
									goto lab4_brk;
								}
								goto lab3_brk;
							}
							while (false);

lab4_brk: ;
							
							cursor = limit - v_4;
							// atlimit, line 40
							if (cursor > limit_backward)
							{
								goto lab2_brk;
							}
						}
						while (false);

lab3_brk: ;
						
						cursor = limit - v_3;
						goto lab1_brk;
					}
					while (false);

lab2_brk: ;
					
					cursor = limit - v_2;
					// (, line 41
					// literal, line 41
					if (!(eq_s_b(1, "e")))
					{
						goto lab0_brk;
					}
					// ], line 41
					bra = cursor;
					// test, line 41
					v_5 = limit - cursor;
					// (, line 41
					// or, line 41
					do 
					{
						v_6 = limit - cursor;
						do 
						{
							if (!(out_grouping_b(g_v, 97, 121)))
							{
								goto lab6_brk;
							}
							goto lab5_brk;
						}
						while (false);

lab6_brk: ;
						
						cursor = limit - v_6;
						// atlimit, line 41
						if (cursor > limit_backward)
						{
							goto lab0_brk;
						}
					}
					while (false);

lab5_brk: ;
					
					// not, line 42
					{
						v_7 = limit - cursor;
						do 
						{
							if (!(in_grouping_b(g_AIOU, 97, 117)))
							{
								goto lab7_brk;
							}
							goto lab0_brk;
						}
						while (false);

lab7_brk: ;
						
						cursor = limit - v_7;
					}
					// not, line 43
					{
						v_8 = limit - cursor;
						do 
						{
							// (, line 43
							// next, line 43
							if (cursor <= limit_backward)
							{
								goto lab8_brk;
							}
							cursor--;
							if (!(in_grouping_b(g_AIOU, 97, 117)))
							{
								goto lab8_brk;
							}
							if (!(out_grouping_b(g_v, 97, 121)))
							{
								goto lab8_brk;
							}
							goto lab0_brk;
						}
						while (false);

lab8_brk: ;
						
						cursor = limit - v_8;
					}
					cursor = limit - v_5;
				}
				while (false);

lab1_brk: ;
				
				// -> ch, line 44
				S_ch = slice_to(S_ch);
				// <+ ch, line 44
				{
					int c = cursor;
					insert(cursor, cursor, S_ch);
					cursor = c;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = limit - v_1;
			return true;
		}
		
		private bool r_Step_1()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			// (, line 48
			// [, line 49
			ket = cursor;
			// among, line 49
			among_var = find_among_b(a_0, 7);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 49
			// ], line 49
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 51
					// delete, line 51
					slice_del();
					break;
				
				case 2: 
					// (, line 52
					// call R1, line 52
					if (!r_R1())
					{
						return false;
					}
					// not, line 52
					{
						v_1 = limit - cursor;
						do 
						{
							// (, line 52
							// literal, line 52
							if (!(eq_s_b(1, "t")))
							{
								goto lab0_brk;
							}
							// call R1, line 52
							if (!r_R1())
							{
								goto lab0_brk;
							}
							return false;
						}
						while (false);

lab0_brk: ;
						
						cursor = limit - v_1;
					}
					// call C, line 52
					if (!r_C())
					{
						return false;
					}
					// delete, line 52
					slice_del();
					break;
				
				case 3: 
					// (, line 53
					// call R1, line 53
					if (!r_R1())
					{
						return false;
					}
					// <-, line 53
					slice_from("ie");
					break;
				
				case 4: 
					// (, line 55
					// or, line 55
					do 
					{
						v_2 = limit - cursor;
						do 
						{
							// (, line 55
							// literal, line 55
							if (!(eq_s_b(2, "ar")))
							{
								goto lab2_brk;
							}
							// call R1, line 55
							if (!r_R1())
							{
								goto lab2_brk;
							}
							// call C, line 55
							if (!r_C())
							{
								goto lab2_brk;
							}
							// ], line 55
							bra = cursor;
							// delete, line 55
							slice_del();
							// call lengthen_V, line 55
							if (!r_lengthen_V())
							{
								goto lab2_brk;
							}
							goto lab1_brk;
						}
						while (false);

lab2_brk: ;
						
						cursor = limit - v_2;
						do 
						{
							// (, line 56
							// literal, line 56
							if (!(eq_s_b(2, "er")))
							{
								goto lab3_brk;
							}
							// call R1, line 56
							if (!r_R1())
							{
								goto lab3_brk;
							}
							// call C, line 56
							if (!r_C())
							{
								goto lab3_brk;
							}
							// ], line 56
							bra = cursor;
							// delete, line 56
							slice_del();
							goto lab1_brk;
						}
						while (false);

lab3_brk: ;
						
						cursor = limit - v_2;
						// (, line 57
						// call R1, line 57
						if (!r_R1())
						{
							return false;
						}
						// call C, line 57
						if (!r_C())
						{
							return false;
						}
						// <-, line 57
						slice_from("e");
					}
					while (false);

lab1_brk: ;

					break;
				
				case 5: 
					// (, line 59
					// call R1, line 59
					if (!r_R1())
					{
						return false;
					}
					// call V, line 59
					if (!r_V())
					{
						return false;
					}
					// <-, line 59
					slice_from("au");
					break;
				
				case 6: 
					// (, line 60
					// or, line 60
					do 
					{
						v_3 = limit - cursor;
						do 
						{
							// (, line 60
							// literal, line 60
							if (!(eq_s_b(3, "hed")))
							{
								goto lab5_brk;
							}
							// call R1, line 60
							if (!r_R1())
							{
								goto lab5_brk;
							}
							// ], line 60
							bra = cursor;
							// <-, line 60
							slice_from("heid");
							goto lab4_brk;
						}
						while (false);

lab5_brk: ;
						
						cursor = limit - v_3;
						do 
						{
							// (, line 61
							// literal, line 61
							if (!(eq_s_b(2, "nd")))
							{
								goto lab6_brk;
							}
							// delete, line 61
							slice_del();
							goto lab4_brk;
						}
						while (false);

lab6_brk: ;
						
						cursor = limit - v_3;
						do 
						{
							// (, line 62
							// literal, line 62
							if (!(eq_s_b(1, "d")))
							{
								goto lab7_brk;
							}
							// call R1, line 62
							if (!r_R1())
							{
								goto lab7_brk;
							}
							// call C, line 62
							if (!r_C())
							{
								goto lab7_brk;
							}
							// ], line 62
							bra = cursor;
							// delete, line 62
							slice_del();
							goto lab4_brk;
						}
						while (false);

lab7_brk: ;
						
						cursor = limit - v_3;
						do 
						{
							// (, line 63
							// or, line 63
							do 
							{
								v_4 = limit - cursor;
								do 
								{
									// literal, line 63
									if (!(eq_s_b(1, "i")))
									{
										goto lab10_brk;
									}
									goto lab9_brk;
								}
								while (false);

lab10_brk: ;
								
								cursor = limit - v_4;
								// literal, line 63
								if (!(eq_s_b(1, "j")))
								{
									goto lab8_brk;
								}
							}
							while (false);

lab9_brk: ;
							
							// call V, line 63
							if (!r_V())
							{
								goto lab8_brk;
							}
							// delete, line 63
							slice_del();
							goto lab4_brk;
						}
						while (false);

lab8_brk: ;
						
						cursor = limit - v_3;
						// (, line 64
						// call R1, line 64
						if (!r_R1())
						{
							return false;
						}
						// call C, line 64
						if (!r_C())
						{
							return false;
						}
						// delete, line 64
						slice_del();
						// call lengthen_V, line 64
						if (!r_lengthen_V())
						{
							return false;
						}
					}
					while (false);

lab4_brk: ;

					break;
				
				case 7: 
					// (, line 65
					// <-, line 65
					slice_from("nd");
					break;
				}
			return true;
		}
		
		private bool r_Step_2()
		{
			int among_var;
			int v_1;
			// (, line 70
			// [, line 71
			ket = cursor;
			// among, line 71
			among_var = find_among_b(a_1, 11);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 71
			// ], line 71
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 72
					// or, line 72
					do 
					{
						v_1 = limit - cursor;
						do 
						{
							// (, line 72
							// literal, line 72
							if (!(eq_s_b(2, "'t")))
							{
								goto lab1_brk;
							}
							// ], line 72
							bra = cursor;
							// delete, line 72
							slice_del();
							goto lab0_brk;
						}
						while (false);

lab1_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 73
							// literal, line 73
							if (!(eq_s_b(2, "et")))
							{
								goto lab2_brk;
							}
							// ], line 73
							bra = cursor;
							// call R1, line 73
							if (!r_R1())
							{
								goto lab2_brk;
							}
							// call C, line 73
							if (!r_C())
							{
								goto lab2_brk;
							}
							// delete, line 73
							slice_del();
							goto lab0_brk;
						}
						while (false);

lab2_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 74
							// literal, line 74
							if (!(eq_s_b(3, "rnt")))
							{
								goto lab3_brk;
							}
							// ], line 74
							bra = cursor;
							// <-, line 74
							slice_from("rn");
							goto lab0_brk;
						}
						while (false);

lab3_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 75
							// literal, line 75
							if (!(eq_s_b(1, "t")))
							{
								goto lab4_brk;
							}
							// ], line 75
							bra = cursor;
							// call R1, line 75
							if (!r_R1())
							{
								goto lab4_brk;
							}
							// call VX, line 75
							if (!r_VX())
							{
								goto lab4_brk;
							}
							// delete, line 75
							slice_del();
							goto lab0_brk;
						}
						while (false);

lab4_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 76
							// literal, line 76
							if (!(eq_s_b(3, "ink")))
							{
								goto lab5_brk;
							}
							// ], line 76
							bra = cursor;
							// <-, line 76
							slice_from("ing");
							goto lab0_brk;
						}
						while (false);

lab5_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 77
							// literal, line 77
							if (!(eq_s_b(2, "mp")))
							{
								goto lab6_brk;
							}
							// ], line 77
							bra = cursor;
							// <-, line 77
							slice_from("m");
							goto lab0_brk;
						}
						while (false);

lab6_brk: ;
						
						cursor = limit - v_1;
						do 
						{
							// (, line 78
							// literal, line 78
							if (!(eq_s_b(1, "'")))
							{
								goto lab7_brk;
							}
							// ], line 78
							bra = cursor;
							// call R1, line 78
							if (!r_R1())
							{
								goto lab7_brk;
							}
							// delete, line 78
							slice_del();
							goto lab0_brk;
						}
						while (false);

lab7_brk: ;
						
						cursor = limit - v_1;
						// (, line 79
						// ], line 79
						bra = cursor;
						// call R1, line 79
						if (!r_R1())
						{
							return false;
						}
						// call C, line 79
						if (!r_C())
						{
							return false;
						}
						// delete, line 79
						slice_del();
					}
					while (false);

lab0_brk: ;

					break;
				
				case 2: 
					// (, line 80
					// call R1, line 80
					if (!r_R1())
					{
						return false;
					}
					// <-, line 80
					slice_from("g");
					break;
				
				case 3: 
					// (, line 81
					// call R1, line 81
					if (!r_R1())
					{
						return false;
					}
					// <-, line 81
					slice_from("lijk");
					break;
				
				case 4: 
					// (, line 82
					// call R1, line 82
					if (!r_R1())
					{
						return false;
					}
					// <-, line 82
					slice_from("isch");
					break;
				
				case 5: 
					// (, line 83
					// call R1, line 83
					if (!r_R1())
					{
						return false;
					}
					// call C, line 83
					if (!r_C())
					{
						return false;
					}
					// delete, line 83
					slice_del();
					break;
				
				case 6: 
					// (, line 84
					// call R1, line 84
					if (!r_R1())
					{
						return false;
					}
					// <-, line 84
					slice_from("t");
					break;
				
				case 7: 
					// (, line 85
					// call R1, line 85
					if (!r_R1())
					{
						return false;
					}
					// <-, line 85
					slice_from("s");
					break;
				
				case 8: 
					// (, line 86
					// call R1, line 86
					if (!r_R1())
					{
						return false;
					}
					// <-, line 86
					slice_from("r");
					break;
				
				case 9: 
					// (, line 87
					// call R1, line 87
					if (!r_R1())
					{
						return false;
					}
					// delete, line 87
					slice_del();
					// attach, line 87
					insert(cursor, cursor, "l");
					// call lengthen_V, line 87
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 10: 
					// (, line 88
					// call R1, line 88
					if (!r_R1())
					{
						return false;
					}
					// call C, line 88
					if (!r_C())
					{
						return false;
					}
					// delete, line 88
					slice_del();
					// attach, line 88
					insert(cursor, cursor, "en");
					// call lengthen_V, line 88
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 11: 
					// (, line 89
					// call R1, line 89
					if (!r_R1())
					{
						return false;
					}
					// call C, line 89
					if (!r_C())
					{
						return false;
					}
					// <-, line 89
					slice_from("ief");
					break;
				}
			return true;
		}
		
		private bool r_Step_3()
		{
			int among_var;
			// (, line 94
			// [, line 95
			ket = cursor;
			// among, line 95
			among_var = find_among_b(a_2, 14);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 95
			// ], line 95
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 96
					// call R1, line 96
					if (!r_R1())
					{
						return false;
					}
					// <-, line 96
					slice_from("eer");
					break;
				
				case 2: 
					// (, line 97
					// call R1, line 97
					if (!r_R1())
					{
						return false;
					}
					// delete, line 97
					slice_del();
					// call lengthen_V, line 97
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 3: 
					// (, line 100
					// call R1, line 100
					if (!r_R1())
					{
						return false;
					}
					// delete, line 100
					slice_del();
					break;
				
				case 4: 
					// (, line 101
					// <-, line 101
					slice_from("r");
					break;
				
				case 5: 
					// (, line 104
					// call R1, line 104
					if (!r_R1())
					{
						return false;
					}
					// delete, line 104
					slice_del();
					// call lengthen_V, line 104
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 6: 
					// (, line 105
					// call R1, line 105
					if (!r_R1())
					{
						return false;
					}
					// call C, line 105
					if (!r_C())
					{
						return false;
					}
					// <-, line 105
					slice_from("aar");
					break;
				
				case 7: 
					// (, line 106
					// call R2, line 106
					if (!r_R2())
					{
						return false;
					}
					// delete, line 106
					slice_del();
					// attach, line 106
					insert(cursor, cursor, "f");
					// call lengthen_V, line 106
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 8: 
					// (, line 107
					// call R2, line 107
					if (!r_R2())
					{
						return false;
					}
					// delete, line 107
					slice_del();
					// attach, line 107
					insert(cursor, cursor, "g");
					// call lengthen_V, line 107
					if (!r_lengthen_V())
					{
						return false;
					}
					break;
				
				case 9: 
					// (, line 108
					// call R1, line 108
					if (!r_R1())
					{
						return false;
					}
					// call C, line 108
					if (!r_C())
					{
						return false;
					}
					// <-, line 108
					slice_from("t");
					break;
				
				case 10: 
					// (, line 109
					// call R1, line 109
					if (!r_R1())
					{
						return false;
					}
					// call C, line 109
					if (!r_C())
					{
						return false;
					}
					// <-, line 109
					slice_from("d");
					break;
				}
			return true;
		}
		
		private bool r_Step_4()
		{
			int among_var;
			int v_1;
			// (, line 114
			// or, line 134
lab11: 
			do 
			{
				v_1 = limit - cursor;
				do 
				{
					// (, line 115
					// [, line 115
					ket = cursor;
					// among, line 115
					among_var = find_among_b(a_3, 16);
					if (among_var == 0)
					{
						goto lab11_brk;
					}
					// (, line 115
					// ], line 115
					bra = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab11_brk;
						
						case 1: 
							// (, line 116
							// call R1, line 116
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// <-, line 116
							slice_from("ie");
							break;
						
						case 2: 
							// (, line 117
							// call R1, line 117
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// <-, line 117
							slice_from("eer");
							break;
						
						case 3: 
							// (, line 118
							// call R1, line 118
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// delete, line 118
							slice_del();
							break;
						
						case 4: 
							// (, line 119
							// call R1, line 119
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// call V, line 119
							if (!r_V())
							{
								goto lab11_brk;
							}
							// <-, line 119
							slice_from("n");
							break;
						
						case 5: 
							// (, line 120
							// call R1, line 120
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// call V, line 120
							if (!r_V())
							{
								goto lab11_brk;
							}
							// <-, line 120
							slice_from("l");
							break;
						
						case 6: 
							// (, line 121
							// call R1, line 121
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// call V, line 121
							if (!r_V())
							{
								goto lab11_brk;
							}
							// <-, line 121
							slice_from("r");
							break;
						
						case 7: 
							// (, line 122
							// call R1, line 122
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// <-, line 122
							slice_from("teer");
							break;
						
						case 8: 
							// (, line 124
							// call R1, line 124
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// <-, line 124
							slice_from("lijk");
							break;
						
						case 9: 
							// (, line 127
							// call R1, line 127
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// delete, line 127
							slice_del();
							break;
						
						case 10: 
							// (, line 131
							// call R1, line 131
							if (!r_R1())
							{
								goto lab11_brk;
							}
							// call C, line 131
							if (!r_C())
							{
								goto lab11_brk;
							}
							// delete, line 131
							slice_del();
							// call lengthen_V, line 131
							if (!r_lengthen_V())
							{
								goto lab11_brk;
							}
							break;
						}
					goto lab11_brk;
				}
				while (false);

lab11_brk: ;
				
				cursor = limit - v_1;
				// (, line 135
				// [, line 135
				ket = cursor;
				// among, line 135
				among_var = find_among_b(a_4, 3);
				if (among_var == 0)
				{
					return false;
				}
				// (, line 135
				// ], line 135
				bra = cursor;
				switch (among_var)
				{
					
					case 0: 
						return false;
					
					case 1: 
						// (, line 138
						// call R1, line 138
						if (!r_R1())
						{
							return false;
						}
						// call C, line 138
						if (!r_C())
						{
							return false;
						}
						// delete, line 138
						slice_del();
						// call lengthen_V, line 138
						if (!r_lengthen_V())
						{
							return false;
						}
						break;
					}
			}
			while (false);
			return true;
		}
		
		private bool r_Step_7()
		{
			int among_var;
			// (, line 144
			// [, line 145
			ket = cursor;
			// among, line 145
			among_var = find_among_b(a_5, 3);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 145
			// ], line 145
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 146
					// <-, line 146
					slice_from("k");
					break;
				
				case 2: 
					// (, line 147
					// <-, line 147
					slice_from("f");
					break;
				
				case 3: 
					// (, line 148
					// <-, line 148
					slice_from("p");
					break;
				}
			return true;
		}
		
		private bool r_Step_6()
		{
			int among_var;
			// (, line 153
			// [, line 154
			ket = cursor;
			// among, line 154
			among_var = find_among_b(a_6, 22);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 154
			// ], line 154
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 155
					// <-, line 155
					slice_from("b");
					break;
				
				case 2: 
					// (, line 156
					// <-, line 156
					slice_from("c");
					break;
				
				case 3: 
					// (, line 157
					// <-, line 157
					slice_from("d");
					break;
				
				case 4: 
					// (, line 158
					// <-, line 158
					slice_from("f");
					break;
				
				case 5: 
					// (, line 159
					// <-, line 159
					slice_from("g");
					break;
				
				case 6: 
					// (, line 160
					// <-, line 160
					slice_from("h");
					break;
				
				case 7: 
					// (, line 161
					// <-, line 161
					slice_from("j");
					break;
				
				case 8: 
					// (, line 162
					// <-, line 162
					slice_from("k");
					break;
				
				case 9: 
					// (, line 163
					// <-, line 163
					slice_from("l");
					break;
				
				case 10: 
					// (, line 164
					// <-, line 164
					slice_from("m");
					break;
				
				case 11: 
					// (, line 165
					// <-, line 165
					slice_from("n");
					break;
				
				case 12: 
					// (, line 166
					// <-, line 166
					slice_from("p");
					break;
				
				case 13: 
					// (, line 167
					// <-, line 167
					slice_from("q");
					break;
				
				case 14: 
					// (, line 168
					// <-, line 168
					slice_from("r");
					break;
				
				case 15: 
					// (, line 169
					// <-, line 169
					slice_from("s");
					break;
				
				case 16: 
					// (, line 170
					// <-, line 170
					slice_from("t");
					break;
				
				case 17: 
					// (, line 171
					// <-, line 171
					slice_from("v");
					break;
				
				case 18: 
					// (, line 172
					// <-, line 172
					slice_from("w");
					break;
				
				case 19: 
					// (, line 173
					// <-, line 173
					slice_from("x");
					break;
				
				case 20: 
					// (, line 174
					// <-, line 174
					slice_from("z");
					break;
				
				case 21: 
					// (, line 175
					// <-, line 175
					slice_from("f");
					break;
				
				case 22: 
					// (, line 176
					// <-, line 176
					slice_from("s");
					break;
				}
			return true;
		}
		
		private bool r_Step_1c()
		{
			int among_var;
			int v_1;
			int v_2;
			// (, line 181
			// [, line 182
			ket = cursor;
			// among, line 182
			among_var = find_among_b(a_7, 2);
			if (among_var == 0)
			{
				return false;
			}
			// (, line 182
			// ], line 182
			bra = cursor;
			// call R1, line 182
			if (!r_R1())
			{
				return false;
			}
			// call C, line 182
			if (!r_C())
			{
				return false;
			}
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 183
					// not, line 183
					{
						v_1 = limit - cursor;
						do 
						{
							// (, line 183
							// literal, line 183
							if (!(eq_s_b(1, "n")))
							{
								goto lab11_brk;
							}
							// call R1, line 183
							if (!r_R1())
							{
								goto lab11_brk;
							}
							return false;
						}
						while (false);

lab11_brk: ;
						
						cursor = limit - v_1;
					}
					// delete, line 183
					slice_del();
					break;
				
				case 2: 
					// (, line 184
					// not, line 184
					{
						v_2 = limit - cursor;
						do 
						{
							// (, line 184
							// literal, line 184
							if (!(eq_s_b(1, "h")))
							{
								goto lab11_brk;
							}
							// call R1, line 184
							if (!r_R1())
							{
								goto lab11_brk;
							}
							return false;
						}
						while (false);

lab11_brk: ;
						
						cursor = limit - v_2;
					}
					// delete, line 184
					slice_del();
					break;
				}
			return true;
		}
		
		private bool r_Lose_prefix()
		{
			int v_1;
			int v_2;
			int v_3;
			// (, line 189
			// [, line 190
			bra = cursor;
			// literal, line 190
			if (!(eq_s(2, "ge")))
			{
				return false;
			}
			// ], line 190
			ket = cursor;
			// test, line 190
			v_1 = cursor;
			// hop, line 190
			{
				int c = cursor + 3;
				if (0 > c || c > limit)
				{
					return false;
				}
				cursor = c;
			}
			cursor = v_1;
			// (, line 190
			// goto, line 190
			while (true)
			{
				v_2 = cursor;
				do 
				{
					if (!(in_grouping(g_v, 97, 121)))
					{
						goto lab11_brk;
					}
					cursor = v_2;
					goto golab0_brk;
				}
				while (false);

lab11_brk: ;
				
				cursor = v_2;
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab0_brk: ;
			
			// goto, line 190
			while (true)
			{
				v_3 = cursor;
				do 
				{
					if (!(out_grouping(g_v, 97, 121)))
					{
						goto lab11_brk;
					}
					cursor = v_3;
					goto golab2_brk;
				}
				while (false);

lab11_brk: ;
				
				cursor = v_3;
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab2_brk: ;
			
			// set GE_removed, line 191
			B_GE_removed = true;
			// delete, line 192
			slice_del();
			return true;
		}
		
		private bool r_Lose_infix()
		{
			int v_2;
			int v_3;
			int v_4;
			// (, line 195
			// next, line 196
			if (cursor >= limit)
			{
				return false;
			}
			cursor++;
			// gopast, line 197
			while (true)
			{
				do 
				{
					// (, line 197
					// [, line 197
					bra = cursor;
					// literal, line 197
					if (!(eq_s(2, "ge")))
					{
						goto lab11_brk;
					}
					// ], line 197
					ket = cursor;
					goto golab1_brk;
				}
				while (false);

lab11_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab1_brk: ;
			
			// test, line 197
			v_2 = cursor;
			// hop, line 197
			{
				int c = cursor + 3;
				if (0 > c || c > limit)
				{
					return false;
				}
				cursor = c;
			}
			cursor = v_2;
			// (, line 197
			// goto, line 197
			while (true)
			{
				v_3 = cursor;
				do 
				{
					if (!(in_grouping(g_v, 97, 121)))
					{
						goto lab11_brk;
					}
					cursor = v_3;
					goto golab3_brk;
				}
				while (false);

lab11_brk: ;
				
				cursor = v_3;
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab3_brk: ;
			
			// goto, line 197
			while (true)
			{
				v_4 = cursor;
				do 
				{
					if (!(out_grouping(g_v, 97, 121)))
					{
						goto lab11_brk;
					}
					cursor = v_4;
					goto golab4_brk;
				}
				while (false);

lab11_brk: ;
				
				cursor = v_4;
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab4_brk: ;
			
			// set GE_removed, line 198
			B_GE_removed = true;
			// delete, line 199
			slice_del();
			return true;
		}
		
		private bool r_measure()
		{
			int v_1;
			int v_2;
			int v_5;
			int v_6;
			int v_9;
			int v_10;
			// (, line 202
			// do, line 203
			v_1 = cursor;
			do 
			{
				// (, line 203
				// tolimit, line 204
				cursor = limit;
				// setmark p1, line 205
				I_p1 = cursor;
				// setmark p2, line 206
				I_p2 = cursor;
			}
			while (false);

lab0_brk: ;

			cursor = v_1;
			// do, line 208
			v_2 = cursor;
			do 
			{
				// (, line 208
				// repeat, line 209
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 121)))
						{
							goto lab3_brk;
						}
						goto replab2;
					}
					while (false);

lab3_brk: ;
					
					goto replab2_brk;

replab2: ;
				}

replab2_brk: ;
				
				// atleast, line 209
				{
					int v_4 = 1;
					// atleast, line 209
					while (true)
					{
						v_5 = cursor;
						do 
						{
							// (, line 209
							// or, line 209
							do 
							{
								v_6 = cursor;
								do 
								{
									// literal, line 209
									if (!(eq_s(2, "ij")))
									{
										goto lab7_brk;
									}
									goto lab6_brk;
								}
								while (false);

lab7_brk: ;
								
								cursor = v_6;
								if (!(in_grouping(g_v, 97, 121)))
								{
									goto lab5_brk;
								}
							}
							while (false);

lab6_brk: ;
							
							v_4--;
							goto replab4;
						}
						while (false);

lab5_brk: ;

						cursor = v_5;
						goto replab4_brk;

replab4: ;
					}

replab4_brk: ;
					
					if (v_4 > 0)
					{
						goto lab1_brk;
					}
				}
				if (!(out_grouping(g_v, 97, 121)))
				{
					goto lab1_brk;
				}
				// setmark p1, line 209
				I_p1 = cursor;
				// repeat, line 210
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 121)))
						{
							goto lab9_brk;
						}
						goto replab8;
					}
					while (false);

lab9_brk: ;
					
					goto replab8_brk;

replab8: ;
				}

replab8_brk: ;
				
				// atleast, line 210
				{
					int v_8 = 1;
					// atleast, line 210
					while (true)
					{
						v_9 = cursor;
						do 
						{
							// (, line 210
							// or, line 210
							do 
							{
								v_10 = cursor;
								do 
								{
									// literal, line 210
									if (!(eq_s(2, "ij")))
									{
										goto lab13_brk;
									}
									goto lab12_brk;
								}
								while (false);

lab13_brk: ;
								
								cursor = v_10;
								if (!(in_grouping(g_v, 97, 121)))
								{
									goto lab11_brk;
								}
							}
							while (false);

lab12_brk: ;
							
							v_8--;
							goto replab10;
						}
						while (false);

lab11_brk: ;
						
						cursor = v_9;
						goto replab10_brk;

replab10: ;
					}

replab10_brk: ;
					
					if (v_8 > 0)
					{
						goto lab1_brk;
					}
				}
				if (!(out_grouping(g_v, 97, 121)))
				{
					goto lab1_brk;
				}
				// setmark p2, line 210
				I_p2 = cursor;
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
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
			int v_9;
			int v_10;
			int v_11;
			int v_12;
			int v_13;
			int v_14;
			int v_15;
			int v_16;
			int v_18;
			int v_19;
			int v_20;
			// (, line 214
			// unset Y_found, line 216
			B_Y_found = false;
			// unset stemmed, line 217
			B_stemmed = false;
			// do, line 218
			v_1 = cursor;
			do 
			{
				// (, line 218
				// [, line 218
				bra = cursor;
				// literal, line 218
				if (!(eq_s(1, "y")))
				{
					goto lab0_brk;
				}
				// ], line 218
				ket = cursor;
				// <-, line 218
				slice_from("Y");
				// set Y_found, line 218
				B_Y_found = true;
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 219
			v_2 = cursor;
			do 
			{
				// repeat, line 219
				while (true)
				{
					v_3 = cursor;
					do 
					{
						// (, line 219
						// goto, line 219
						while (true)
						{
							v_4 = cursor;
							do 
							{
								// (, line 219
								if (!(in_grouping(g_v, 97, 121)))
								{
									goto lab5_brk;
								}
								// [, line 219
								bra = cursor;
								// literal, line 219
								if (!(eq_s(1, "y")))
								{
									goto lab5_brk;
								}
								// ], line 219
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
						
						// <-, line 219
						slice_from("Y");
						// set Y_found, line 219
						B_Y_found = true;
						goto replab3;
					}
					while (false);

lab3_brk: ;
					
					cursor = v_3;
					goto replab3_brk;

replab3: ;
				}

replab3_brk: ;
				
			}
			while (false);

lab1_brk: ;

			cursor = v_2;
			// call measure, line 221
			if (!r_measure())
			{
				return false;
			}
			// backwards, line 223
			limit_backward = cursor; cursor = limit;
			// (, line 223
			// do, line 224
			v_5 = limit - cursor;
			do 
			{
				// (, line 224
				// call Step_1, line 224
				if (!r_Step_1())
				{
					goto lab6_brk;
				}
				// set stemmed, line 224
				B_stemmed = true;
			}
			while (false);

lab6_brk: ;
			
			cursor = limit - v_5;
			// do, line 225
			v_6 = limit - cursor;
			do 
			{
				// (, line 225
				// call Step_2, line 225
				if (!r_Step_2())
				{
					goto lab7_brk;
				}
				// set stemmed, line 225
				B_stemmed = true;
			}
			while (false);

lab7_brk: ;
			
			cursor = limit - v_6;
			// do, line 226
			v_7 = limit - cursor;
			do 
			{
				// (, line 226
				// call Step_3, line 226
				if (!r_Step_3())
				{
					goto lab8_brk;
				}
				// set stemmed, line 226
				B_stemmed = true;
			}
			while (false);

lab8_brk: ;
			
			cursor = limit - v_7;
			// do, line 227
			v_8 = limit - cursor;
			do 
			{
				// (, line 227
				// call Step_4, line 227
				if (!r_Step_4())
				{
					goto lab9_brk;
				}
				// set stemmed, line 227
				B_stemmed = true;
			}
			while (false);

lab9_brk: ;
			
			cursor = limit - v_8;
			cursor = limit_backward; // unset GE_removed, line 229
			B_GE_removed = false;
			// do, line 230
			v_9 = cursor;
			do 
			{
				// (, line 230
				// and, line 230
				v_10 = cursor;
				// call Lose_prefix, line 230
				if (!r_Lose_prefix())
				{
					goto lab10_brk;
				}
				cursor = v_10;
				// call measure, line 230
				if (!r_measure())
				{
					goto lab10_brk;
				}
			}
			while (false);

lab10_brk: ;
			
			cursor = v_9;
			// backwards, line 231
			limit_backward = cursor; cursor = limit;
			// (, line 231
			// do, line 232
			v_11 = limit - cursor;
			do 
			{
				// (, line 232
				// Boolean test GE_removed, line 232
				if (!(B_GE_removed))
				{
					goto lab11_brk;
				}
				// call Step_1c, line 232
				if (!r_Step_1c())
				{
					goto lab11_brk;
				}
			}
			while (false);

lab11_brk: ;
			
			cursor = limit - v_11;
			cursor = limit_backward; // unset GE_removed, line 234
			B_GE_removed = false;
			// do, line 235
			v_12 = cursor;
			do 
			{
				// (, line 235
				// and, line 235
				v_13 = cursor;
				// call Lose_infix, line 235
				if (!r_Lose_infix())
				{
					goto lab12_brk;
				}
				cursor = v_13;
				// call measure, line 235
				if (!r_measure())
				{
					goto lab12_brk;
				}
			}
			while (false);

lab12_brk: ;
			
			cursor = v_12;
			// backwards, line 236
			limit_backward = cursor; cursor = limit;
			// (, line 236
			// do, line 237
			v_14 = limit - cursor;
			do 
			{
				// (, line 237
				// Boolean test GE_removed, line 237
				if (!(B_GE_removed))
				{
					goto lab13_brk;
				}
				// call Step_1c, line 237
				if (!r_Step_1c())
				{
					goto lab13_brk;
				}
			}
			while (false);

lab13_brk: ;
			
			cursor = limit - v_14;
			cursor = limit_backward; // backwards, line 239
			limit_backward = cursor; cursor = limit;
			// (, line 239
			// do, line 240
			v_15 = limit - cursor;
			do 
			{
				// (, line 240
				// call Step_7, line 240
				if (!r_Step_7())
				{
					goto lab14_brk;
				}
				// set stemmed, line 240
				B_stemmed = true;
			}
			while (false);

lab14_brk: ;
			
			cursor = limit - v_15;
			// do, line 241
			v_16 = limit - cursor;
			do 
			{
				// (, line 241
				// or, line 241
				do 
				{
					do 
					{
						// Boolean test stemmed, line 241
						if (!(B_stemmed))
						{
							goto lab17_brk;
						}
						goto lab16_brk;
					}
					while (false);

lab17_brk: ;
					
					// Boolean test GE_removed, line 241
					if (!(B_GE_removed))
					{
						goto lab15_brk;
					}
				}
				while (false);

lab16_brk: ;
				
				// call Step_6, line 241
				if (!r_Step_6())
				{
					goto lab15_brk;
				}
			}
			while (false);

lab15_brk: ;
			
			cursor = limit - v_16;
			cursor = limit_backward; // do, line 243
			v_18 = cursor;
			do 
			{
				// (, line 243
				// Boolean test Y_found, line 243
				if (!(B_Y_found))
				{
					goto lab18_brk;
				}
				// repeat, line 243
				while (true)
				{
					v_19 = cursor;
					do 
					{
						// (, line 243
						// goto, line 243
						while (true)
						{
							v_20 = cursor;
							do 
							{
								// (, line 243
								// [, line 243
								bra = cursor;
								// literal, line 243
								if (!(eq_s(1, "Y")))
								{
									goto lab22_brk;
								}
								// ], line 243
								ket = cursor;
								cursor = v_20;
								goto golab21_brk;
							}
							while (false);

lab22_brk: ;
							
							cursor = v_20;
							if (cursor >= limit)
							{
								goto lab20_brk;
							}
							cursor++;
						}

golab21_brk: ;
						
						// <-, line 243
						slice_from("y");
						goto replab19;
					}
					while (false);

lab20_brk: ;
					
					cursor = v_19;
					goto replab19_brk;

replab19: ;
				}

replab19_brk: ;
				
			}
			while (false);

lab18_brk: ;
			
			cursor = v_18;
			return true;
		}
	}
}