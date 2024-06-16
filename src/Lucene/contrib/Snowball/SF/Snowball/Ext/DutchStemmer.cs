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
#pragma warning disable 162
    
    /// <summary> Generated class implementing code defined by a snowball script.</summary>
	public class DutchStemmer : SnowballProgram
	{
		public DutchStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("", - 1, 6, "", this), new Among("\u00E1", 0, 1, "", this), new Among("\u00E4", 0, 1, "", this), new Among("\u00E9", 0, 2, "", this), new Among("\u00EB", 0, 2, "", this), new Among("\u00ED", 0, 3, "", this), new Among("\u00EF", 0, 3, "", this), new Among("\u00F3", 0, 4, "", this), new Among("\u00F6", 0, 4, "", this), new Among("\u00FA", 0, 5, "", this), new Among("\u00FC", 0, 5, "", this)};
			a_1 = new Among[]{new Among("", - 1, 3, "", this), new Among("I", 0, 2, "", this), new Among("Y", 0, 1, "", this)};
			a_2 = new Among[]{new Among("dd", - 1, - 1, "", this), new Among("kk", - 1, - 1, "", this), new Among("tt", - 1, - 1, "", this)};
			a_3 = new Among[]{new Among("ene", - 1, 2, "", this), new Among("se", - 1, 3, "", this), new Among("en", - 1, 2, "", this), new Among("heden", 2, 1, "", this), new Among("s", - 1, 3, "", this)};
			a_4 = new Among[]{new Among("end", - 1, 1, "", this), new Among("ig", - 1, 2, "", this), new Among("ing", - 1, 1, "", this), new Among("lijk", - 1, 3, "", this), new Among("baar", - 1, 4, "", this), new Among("bar", - 1, 5, "", this)};
			a_5 = new Among[]{new Among("aa", - 1, - 1, "", this), new Among("ee", - 1, - 1, "", this), new Among("oo", - 1, - 1, "", this), new Among("uu", - 1, - 1, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private Among[] a_5;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (128)};
		private static readonly char[] g_v_I = new char[]{(char) (1), (char) (0), (char) (0), (char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (128)};
		private static readonly char[] g_v_j = new char[]{(char) (17), (char) (67), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (128)};
		
		private int I_p2;
		private int I_p1;
		private bool B_e_found;
		
		protected internal virtual void  copy_from(DutchStemmer other)
		{
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
			B_e_found = other.B_e_found;
			base.copy_from(other);
		}
		
		private bool r_prelude()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			int v_5;
			int v_6;
			// (, line 41
			// test, line 42
			v_1 = cursor;
			// repeat, line 42
			while (true)
			{
				v_2 = cursor;
				do 
				{
					// (, line 42
					// [, line 43
					bra = cursor;
					// substring, line 43
					among_var = find_among(a_0, 11);
					if (among_var == 0)
					{
						goto lab1_brk;
					}
					// ], line 43
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab1_brk;
						
						case 1: 
							// (, line 45
							// <-, line 45
							slice_from("a");
							break;
						
						case 2: 
							// (, line 47
							// <-, line 47
							slice_from("e");
							break;
						
						case 3: 
							// (, line 49
							// <-, line 49
							slice_from("i");
							break;
						
						case 4: 
							// (, line 51
							// <-, line 51
							slice_from("o");
							break;
						
						case 5: 
							// (, line 53
							// <-, line 53
							slice_from("u");
							break;
						
						case 6: 
							// (, line 54
							// next, line 54
							if (cursor >= limit)
							{
								goto lab1_brk;
							}
							cursor++;
							break;
						}
					goto replab0;
				}
				while (false);

lab1_brk: ;
				
				cursor = v_2;
				goto replab0_brk;

replab0: ;
			}

replab0_brk: ;
			
			cursor = v_1;
			// try, line 57
			v_3 = cursor;
			do 
			{
				// (, line 57
				// [, line 57
				bra = cursor;
				// literal, line 57
				if (!(eq_s(1, "y")))
				{
					cursor = v_3;
					goto lab2_brk;
				}
				// ], line 57
				ket = cursor;
				// <-, line 57
				slice_from("Y");
			}
			while (false);

lab2_brk: ;
			
			// repeat, line 58
			while (true)
			{
				v_4 = cursor;
				do 
				{
					// goto, line 58
					while (true)
					{
						v_5 = cursor;
						do 
						{
							// (, line 58
							if (!(in_grouping(g_v, 97, 232)))
							{
								goto lab6_brk;
							}
							// [, line 59
							bra = cursor;
							// or, line 59
							do 
							{
								v_6 = cursor;
								do 
								{
									// (, line 59
									// literal, line 59
									if (!(eq_s(1, "i")))
									{
										goto lab8_brk;
									}
									// ], line 59
									ket = cursor;
									if (!(in_grouping(g_v, 97, 232)))
									{
										goto lab8_brk;
									}
									// <-, line 59
									slice_from("I");
									goto lab7_brk;
								}
								while (false);

lab8_brk: ;
								
								cursor = v_6;
								// (, line 60
								// literal, line 60
								if (!(eq_s(1, "y")))
								{
									goto lab6_brk;
								}
								// ], line 60
								ket = cursor;
								// <-, line 60
								slice_from("Y");
							}
							while (false);

lab7_brk: ;
							
							cursor = v_5;
							goto golab5_brk;
						}
						while (false);

lab6_brk: ;
						
						cursor = v_5;
						if (cursor >= limit)
						{
							goto lab4_brk;
						}
						cursor++;
					}

golab5_brk: ;
					
					goto replab3;
				}
				while (false);

lab4_brk: ;
				
				cursor = v_4;
				goto replab3_brk;

replab3: ;
			}

replab3_brk: ;
			
			return true;
		}
		
		private bool r_mark_regions()
		{
			// (, line 64
			I_p1 = limit;
			I_p2 = limit;
			// gopast, line 69
			while (true)
			{
				do 
				{
					if (!(in_grouping(g_v, 97, 232)))
					{
						goto lab3_brk;
					}
					goto golab0_brk;
				}
				while (false);

lab3_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab0_brk: ;
			
			// gopast, line 69
			while (true)
			{
				do 
				{
					if (!(out_grouping(g_v, 97, 232)))
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
			
			// setmark p1, line 69
			I_p1 = cursor;
			// try, line 70
			do 
			{
				// (, line 70
				if (!(I_p1 < 3))
				{
					goto lab5_brk;
				}
				I_p1 = 3;
			}
			while (false);

lab5_brk: ;
			
			// gopast, line 71
			while (true)
			{
				do 
				{
					if (!(in_grouping(g_v, 97, 232)))
					{
						goto lab9_brk;
					}
					goto golab6_brk;
				}
				while (false);

lab9_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab6_brk: ;
			
			// gopast, line 71
			while (true)
			{
				do 
				{
					if (!(out_grouping(g_v, 97, 232)))
					{
						goto lab9_brk;
					}
					goto golab7_brk;
				}
				while (false);

lab9_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab7_brk: ;
			
			// setmark p2, line 71
			I_p2 = cursor;
			return true;
		}
		
		private bool r_postlude()
		{
			int among_var;
			int v_1;
			// repeat, line 75
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 75
					// [, line 77
					bra = cursor;
					// substring, line 77
					among_var = find_among(a_1, 3);
					if (among_var == 0)
					{
						goto lab5_brk;
					}
					// ], line 77
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab5_brk;
						
						case 1: 
							// (, line 78
							// <-, line 78
							slice_from("y");
							break;
						
						case 2: 
							// (, line 79
							// <-, line 79
							slice_from("i");
							break;
						
						case 3: 
							// (, line 80
							// next, line 80
							if (cursor >= limit)
							{
								goto lab5_brk;
							}
							cursor++;
							break;
						}
					goto replab1;
				}
				while (false);

lab5_brk: ;
				
				cursor = v_1;
				goto replab1_brk;

replab1: ;
			}

replab1_brk: ;
			
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
		
		private bool r_undouble()
		{
			int v_1;
			// (, line 90
			// test, line 91
			v_1 = limit - cursor;
			// among, line 91
			if (find_among_b(a_2, 3) == 0)
			{
				return false;
			}
			cursor = limit - v_1;
			// [, line 91
			ket = cursor;
			// next, line 91
			if (cursor <= limit_backward)
			{
				return false;
			}
			cursor--;
			// ], line 91
			bra = cursor;
			// delete, line 91
			slice_del();
			return true;
		}
		
		private bool r_e_ending()
		{
			int v_1;
			// (, line 94
			// unset e_found, line 95
			B_e_found = false;
			// [, line 96
			ket = cursor;
			// literal, line 96
			if (!(eq_s_b(1, "e")))
			{
				return false;
			}
			// ], line 96
			bra = cursor;
			// call R1, line 96
			if (!r_R1())
			{
				return false;
			}
			// test, line 96
			v_1 = limit - cursor;
			if (!(out_grouping_b(g_v, 97, 232)))
			{
				return false;
			}
			cursor = limit - v_1;
			// delete, line 96
			slice_del();
			// set e_found, line 97
			B_e_found = true;
			// call undouble, line 98
			if (!r_undouble())
			{
				return false;
			}
			return true;
		}
		
		private bool r_en_ending()
		{
			int v_1;
			int v_2;
			// (, line 101
			// call R1, line 102
			if (!r_R1())
			{
				return false;
			}
			// and, line 102
			v_1 = limit - cursor;
			if (!(out_grouping_b(g_v, 97, 232)))
			{
				return false;
			}
			cursor = limit - v_1;
			// not, line 102
			{
				v_2 = limit - cursor;
				do 
				{
					// literal, line 102
					if (!(eq_s_b(3, "gem")))
					{
						goto lab0_brk;
					}
					return false;
				}
				while (false);

lab0_brk: ;
				
				cursor = limit - v_2;
			}
			// delete, line 102
			slice_del();
			// call undouble, line 103
			if (!r_undouble())
			{
				return false;
			}
			return true;
		}
		
		private bool r_standard_suffix()
		{
			int among_var;
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
			// (, line 106
			// do, line 107
			v_1 = limit - cursor;
			do 
			{
				// (, line 107
				// [, line 108
				ket = cursor;
				// substring, line 108
				among_var = find_among_b(a_3, 5);
				if (among_var == 0)
				{
					goto lab0_brk;
				}
				// ], line 108
				bra = cursor;
				switch (among_var)
				{
					
					case 0: 
						goto lab0_brk;
					
					case 1: 
						// (, line 110
						// call R1, line 110
						if (!r_R1())
						{
							goto lab0_brk;
						}
						// <-, line 110
						slice_from("heid");
						break;
					
					case 2: 
						// (, line 113
						// call en_ending, line 113
						if (!r_en_ending())
						{
							goto lab0_brk;
						}
						break;
					
					case 3: 
						// (, line 116
						// call R1, line 116
						if (!r_R1())
						{
							goto lab0_brk;
						}
						if (!(out_grouping_b(g_v_j, 97, 232)))
						{
							goto lab0_brk;
						}
						// delete, line 116
						slice_del();
						break;
					}
			}
			while (false);

lab0_brk: ;

			cursor = limit - v_1;
			// do, line 120
			v_2 = limit - cursor;
			do 
			{
				// call e_ending, line 120
				if (!r_e_ending())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;

			cursor = limit - v_2;
			// do, line 122
			v_3 = limit - cursor;
			do 
			{
				// (, line 122
				// [, line 122
				ket = cursor;
				// literal, line 122
				if (!(eq_s_b(4, "heid")))
				{
					goto lab2_brk;
				}
				// ], line 122
				bra = cursor;
				// call R2, line 122
				if (!r_R2())
				{
					goto lab2_brk;
				}
				// not, line 122
				{
					v_4 = limit - cursor;
					do 
					{
						// literal, line 122
						if (!(eq_s_b(1, "c")))
						{
							goto lab3_brk;
						}
						goto lab2_brk;
					}
					while (false);

lab3_brk: ;
					
					cursor = limit - v_4;
				}
				// delete, line 122
				slice_del();
				// [, line 123
				ket = cursor;
				// literal, line 123
				if (!(eq_s_b(2, "en")))
				{
					goto lab2_brk;
				}
				// ], line 123
				bra = cursor;
				// call en_ending, line 123
				if (!r_en_ending())
				{
					goto lab2_brk;
				}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			// do, line 126
			v_5 = limit - cursor;
			do 
			{
				// (, line 126
				// [, line 127
				ket = cursor;
				// substring, line 127
				among_var = find_among_b(a_4, 6);
				if (among_var == 0)
				{
					goto lab4_brk;
				}
				// ], line 127
				bra = cursor;
				switch (among_var)
				{
					
					case 0: 
						goto lab4_brk;
					
					case 1: 
						// (, line 129
						// call R2, line 129
						if (!r_R2())
						{
							goto lab4_brk;
						}
						// delete, line 129
						slice_del();
						// or, line 130
						do 
						{
							v_6 = limit - cursor;
							do 
							{
								// (, line 130
								// [, line 130
								ket = cursor;
								// literal, line 130
								if (!(eq_s_b(2, "ig")))
								{
									goto lab6_brk;
								}
								// ], line 130
								bra = cursor;
								// call R2, line 130
								if (!r_R2())
								{
									goto lab6_brk;
								}
								// not, line 130
								{
									v_7 = limit - cursor;
									do 
									{
										// literal, line 130
										if (!(eq_s_b(1, "e")))
										{
											goto lab7_brk;
										}
										goto lab6_brk;
									}
									while (false);

lab7_brk: ;
									
									cursor = limit - v_7;
								}
								// delete, line 130
								slice_del();
								goto lab5_brk;
							}
							while (false);

lab6_brk: ;
							
							cursor = limit - v_6;
							// call undouble, line 130
							if (!r_undouble())
							{
								goto lab4_brk;
							}
						}
						while (false);

lab5_brk: ;
						
						break;
					
					case 2: 
						// (, line 133
						// call R2, line 133
						if (!r_R2())
						{
							goto lab4_brk;
						}
						// not, line 133
						{
							v_8 = limit - cursor;
							do 
							{
								// literal, line 133
								if (!(eq_s_b(1, "e")))
								{
									goto lab8_brk;
								}
								goto lab4_brk;
							}
							while (false);

lab8_brk: ;

							cursor = limit - v_8;
						}
						// delete, line 133
						slice_del();
						break;
					
					case 3: 
						// (, line 136
						// call R2, line 136
						if (!r_R2())
						{
							goto lab4_brk;
						}
						// delete, line 136
						slice_del();
						// call e_ending, line 136
						if (!r_e_ending())
						{
							goto lab4_brk;
						}
						break;
					
					case 4: 
						// (, line 139
						// call R2, line 139
						if (!r_R2())
						{
							goto lab4_brk;
						}
						// delete, line 139
						slice_del();
						break;
					
					case 5: 
						// (, line 142
						// call R2, line 142
						if (!r_R2())
						{
							goto lab4_brk;
						}
						// Boolean test e_found, line 142
						if (!(B_e_found))
						{
							goto lab4_brk;
						}
						// delete, line 142
						slice_del();
						break;
					}
			}
			while (false);

lab4_brk: ;
			
			cursor = limit - v_5;
			// do, line 146
			v_9 = limit - cursor;
			do 
			{
				// (, line 146
				if (!(out_grouping_b(g_v_I, 73, 232)))
				{
					goto lab9_brk;
				}
				// test, line 148
				v_10 = limit - cursor;
				// (, line 148
				// among, line 149
				if (find_among_b(a_5, 4) == 0)
				{
					goto lab9_brk;
				}
				if (!(out_grouping_b(g_v, 97, 232)))
				{
					goto lab9_brk;
				}
				cursor = limit - v_10;
				// [, line 152
				ket = cursor;
				// next, line 152
				if (cursor <= limit_backward)
				{
					goto lab9_brk;
				}
				cursor--;
				// ], line 152
				bra = cursor;
				// delete, line 152
				slice_del();
			}
			while (false);

lab9_brk: ;
			
			cursor = limit - v_9;
			return true;
		}
		
		public override bool Stem()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			// (, line 157
			// do, line 159
			v_1 = cursor;
			do 
			{
				// call prelude, line 159
				if (!r_prelude())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;

			cursor = v_1;
			// do, line 160
			v_2 = cursor;
			do 
			{
				// call mark_regions, line 160
				if (!r_mark_regions())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
			// backwards, line 161
			limit_backward = cursor; cursor = limit;
			// do, line 162
			v_3 = limit - cursor;
			do 
			{
				// call standard_suffix, line 162
				if (!r_standard_suffix())
				{
					goto lab2_brk;
				}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			cursor = limit_backward; // do, line 163
			v_4 = cursor;
			do 
			{
				// call postlude, line 163
				if (!r_postlude())
				{
					goto lab3_brk;
				}
			}
			while (false);

lab3_brk: ;
			
			cursor = v_4;
			return true;
		}
	}
}
