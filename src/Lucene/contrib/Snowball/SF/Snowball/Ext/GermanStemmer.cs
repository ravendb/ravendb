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
	public class GermanStemmer : SnowballProgram
	{
		public GermanStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("", - 1, 6, "", this), new Among("U", 0, 2, "", this), new Among("Y", 0, 1, "", this), new Among("\u00E4", 0, 3, "", this), new Among("\u00F6", 0, 4, "", this), new Among("\u00FC", 0, 5, "", this)};
			a_1 = new Among[]{new Among("e", - 1, 1, "", this), new Among("em", - 1, 1, "", this), new Among("en", - 1, 1, "", this), new Among("ern", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("s", - 1, 2, "", this), new Among("es", 5, 1, "", this)};
			a_2 = new Among[]{new Among("en", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("st", - 1, 2, "", this), new Among("est", 2, 1, "", this)};
			a_3 = new Among[]{new Among("ig", - 1, 1, "", this), new Among("lich", - 1, 1, "", this)};
			a_4 = new Among[]{new Among("end", - 1, 1, "", this), new Among("ig", - 1, 2, "", this), new Among("ung", - 1, 1, "", this), new Among("lich", - 1, 3, "", this), new Among("isch", - 1, 2, "", this), new Among("ik", - 1, 2, "", this), new Among("heit", - 1, 3, "", this), new Among("keit", - 1, 4, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (8), (char) (0), (char) (32), (char) (8)};
		private static readonly char[] g_s_ending = new char[]{(char) (117), (char) (30), (char) (5)};
		private static readonly char[] g_st_ending = new char[]{(char) (117), (char) (30), (char) (4)};
		
		private int I_p2;
		private int I_p1;
		
		protected internal virtual void  copy_from(GermanStemmer other)
		{
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
			base.copy_from(other);
		}
		
		private bool r_prelude()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			int v_5;
			int v_6;
			// (, line 28
			// test, line 30
			v_1 = cursor;
			// repeat, line 30
			while (true)
			{
				v_2 = cursor;
				do 
				{
					// (, line 30
					// or, line 33
					do 
					{
						v_3 = cursor;
						do 
						{
							// (, line 31
							// [, line 32
							bra = cursor;
							// literal, line 32
							if (!(eq_s(1, "\u00DF")))
							{
								goto lab3_brk;
							}
							// ], line 32
							ket = cursor;
							// <-, line 32
							slice_from("ss");
							goto lab2_brk;
						}
						while (false);

lab3_brk: ;
						
						cursor = v_3;
						// next, line 33
						if (cursor >= limit)
						{
							goto lab1_brk;
						}
						cursor++;
					}
					while (false);

lab2_brk: ;
					
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
			// repeat, line 36
			while (true)
			{
				v_4 = cursor;
				do 
				{
					// goto, line 36
					while (true)
					{
						v_5 = cursor;
						do 
						{
							// (, line 36
							if (!(in_grouping(g_v, 97, 252)))
							{
								goto lab7_brk;
							}
							// [, line 37
							bra = cursor;
							// or, line 37
							do 
							{
								v_6 = cursor;
								do 
								{
									// (, line 37
									// literal, line 37
									if (!(eq_s(1, "u")))
									{
										goto lab9_brk;
									}
									// ], line 37
									ket = cursor;
									if (!(in_grouping(g_v, 97, 252)))
									{
										goto lab9_brk;
									}
									// <-, line 37
									slice_from("U");
									goto lab8_brk;
								}
								while (false);

lab9_brk: ;
								
								cursor = v_6;
								// (, line 38
								// literal, line 38
								if (!(eq_s(1, "y")))
								{
									goto lab7_brk;
								}
								// ], line 38
								ket = cursor;
								if (!(in_grouping(g_v, 97, 252)))
								{
									goto lab7_brk;
								}
								// <-, line 38
								slice_from("Y");
							}
							while (false);

lab8_brk: ;
							
							cursor = v_5;
							goto golab6_brk;
						}
						while (false);

lab7_brk: ;
						
						cursor = v_5;
						if (cursor >= limit)
						{
							goto lab5_brk;
						}
						cursor++;
					}

golab6_brk: ;
					
					goto replab4;
				}
				while (false);

lab5_brk: ;
				
				cursor = v_4;
				goto replab4_brk;

replab4: ;
			}

replab4_brk: ;
			
			return true;
		}
		
		private bool r_mark_regions()
		{
			// (, line 42
			I_p1 = limit;
			I_p2 = limit;
			// gopast, line 47
			while (true)
			{
				do 
				{
					if (!(in_grouping(g_v, 97, 252)))
					{
						goto lab1_brk;
					}
					goto golab0_brk;
				}
				while (false);

lab1_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab0_brk: ;
			
			// gopast, line 47
			while (true)
			{
				do 
				{
					if (!(out_grouping(g_v, 97, 252)))
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
			
			// setmark p1, line 47
			I_p1 = cursor;
			// try, line 48
			do 
			{
				// (, line 48
				if (!(I_p1 < 3))
				{
					goto lab4_brk;
				}
				I_p1 = 3;
			}
			while (false);

lab4_brk: ;
			
			// gopast, line 49
			while (true)
			{
				do 
				{
					if (!(in_grouping(g_v, 97, 252)))
					{
						goto lab6_brk;
					}
					goto golab5_brk;
				}
				while (false);

lab6_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab5_brk: ;
			
			// gopast, line 49
			while (true)
			{
				do 
				{
					if (!(out_grouping(g_v, 97, 252)))
					{
						goto lab8_brk;
					}
					goto golab7_brk;
				}
				while (false);

lab8_brk: ;
				
				if (cursor >= limit)
				{
					return false;
				}
				cursor++;
			}

golab7_brk: ;
			
			// setmark p2, line 49
			I_p2 = cursor;
			return true;
		}
		
		private bool r_postlude()
		{
			int among_var;
			int v_1;
			// repeat, line 53
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 53
					// [, line 55
					bra = cursor;
					// substring, line 55
					among_var = find_among(a_0, 6);
					if (among_var == 0)
					{
						goto lab10_brk;
					}
					// ], line 55
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab10_brk;
						
						case 1: 
							// (, line 56
							// <-, line 56
							slice_from("y");
							break;
						
						case 2: 
							// (, line 57
							// <-, line 57
							slice_from("u");
							break;
						
						case 3: 
							// (, line 58
							// <-, line 58
							slice_from("a");
							break;
						
						case 4: 
							// (, line 59
							// <-, line 59
							slice_from("o");
							break;
						
						case 5: 
							// (, line 60
							// <-, line 60
							slice_from("u");
							break;
						
						case 6: 
							// (, line 61
							// next, line 61
							if (cursor >= limit)
							{
								goto lab10_brk;
							}
							cursor++;
							break;
						}
					goto replab1;
				}
				while (false);

lab10_brk: ;
				
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
			// (, line 71
			// do, line 72
			v_1 = limit - cursor;
			do 
			{
				// (, line 72
				// [, line 73
				ket = cursor;
				// substring, line 73
				among_var = find_among_b(a_1, 7);
				if (among_var == 0)
				{
					goto lab0_brk;
				}
				// ], line 73
				bra = cursor;
				// call R1, line 73
				if (!r_R1())
				{
					goto lab0_brk;
				}
				switch (among_var)
				{
					
					case 0: 
						goto lab0_brk;
					
					case 1: 
						// (, line 75
						// delete, line 75
						slice_del();
						break;
					
					case 2: 
						// (, line 78
						if (!(in_grouping_b(g_s_ending, 98, 116)))
						{
							goto lab0_brk;
						}
						// delete, line 78
						slice_del();
						break;
					}
			}
			while (false);

lab0_brk: ;
			
			cursor = limit - v_1;
			// do, line 82
			v_2 = limit - cursor;
			do 
			{
				// (, line 82
				// [, line 83
				ket = cursor;
				// substring, line 83
				among_var = find_among_b(a_2, 4);
				if (among_var == 0)
				{
					goto lab1_brk;
				}
				// ], line 83
				bra = cursor;
				// call R1, line 83
				if (!r_R1())
				{
					goto lab1_brk;
				}
				switch (among_var)
				{
					
					case 0: 
						goto lab1_brk;
					
					case 1: 
						// (, line 85
						// delete, line 85
						slice_del();
						break;
					
					case 2: 
						// (, line 88
						if (!(in_grouping_b(g_st_ending, 98, 116)))
						{
							goto lab1_brk;
						}
						// hop, line 88
						{
							int c = cursor - 3;
							if (limit_backward > c || c > limit)
							{
								goto lab1_brk;
							}
							cursor = c;
						}
						// delete, line 88
						slice_del();
						break;
					}
			}
			while (false);

lab1_brk: ;
			
			cursor = limit - v_2;
			// do, line 92
			v_3 = limit - cursor;
			do 
			{
				// (, line 92
				// [, line 93
				ket = cursor;
				// substring, line 93
				among_var = find_among_b(a_4, 8);
				if (among_var == 0)
				{
					goto lab2_brk;
				}
				// ], line 93
				bra = cursor;
				// call R2, line 93
				if (!r_R2())
				{
					goto lab2_brk;
				}
				switch (among_var)
				{
					
					case 0: 

                        goto lab2_brk;
					
					case 1: 
						// (, line 95
						// delete, line 95
						slice_del();
						// try, line 96
						v_4 = limit - cursor;
						do 
						{
							// (, line 96
							// [, line 96
							ket = cursor;
							// literal, line 96
							if (!(eq_s_b(2, "ig")))
							{
								cursor = limit - v_4;
								goto lab3_brk;
							}
							// ], line 96
							bra = cursor;
							// not, line 96
							{
								v_5 = limit - cursor;
								do 
								{
									// literal, line 96
									if (!(eq_s_b(1, "e")))
									{
										goto lab4_brk;
									}
									cursor = limit - v_4;
									goto lab3_brk;
								}
								while (false);

lab4_brk: ;
								
								cursor = limit - v_5;
							}
							// call R2, line 96
							if (!r_R2())
							{
								cursor = limit - v_4;
								goto lab3_brk;
							}
							// delete, line 96
							slice_del();
						}
						while (false);

lab3_brk: ;
						
						break;
					
					case 2: 
						// (, line 99
						// not, line 99
						{
							v_6 = limit - cursor;
							do 
							{
								// literal, line 99
								if (!(eq_s_b(1, "e")))
								{
									goto lab5_brk;
								}
								goto lab2_brk;
							}
							while (false);

lab5_brk: ;
							
							cursor = limit - v_6;
						}
						// delete, line 99
						slice_del();
						break;
					
					case 3: 
						// (, line 102
						// delete, line 102
						slice_del();
						// try, line 103
						v_7 = limit - cursor;
						do 
						{
							// (, line 103
							// [, line 104
							ket = cursor;
							// or, line 104
							do 
							{
								v_8 = limit - cursor;
								do 
								{
									// literal, line 104
									if (!(eq_s_b(2, "er")))
									{
										goto lab8_brk;
									}
									goto lab7_brk;
								}
								while (false);

lab8_brk: ;
								
								cursor = limit - v_8;
								// literal, line 104
								if (!(eq_s_b(2, "en")))
								{
									cursor = limit - v_7;
									goto lab6_brk;
								}
							}
							while (false);

lab7_brk: ;
							
							// ], line 104
							bra = cursor;
							// call R1, line 104
							if (!r_R1())
							{
								cursor = limit - v_7;
								goto lab6_brk;
							}
							// delete, line 104
							slice_del();
						}
						while (false);

lab6_brk: ;
						
						break;
					
					case 4: 
						// (, line 108
						// delete, line 108
						slice_del();
						// try, line 109
						v_9 = limit - cursor;
						do 
						{
							// (, line 109
							// [, line 110
							ket = cursor;
							// substring, line 110
							among_var = find_among_b(a_3, 2);
							if (among_var == 0)
							{
								cursor = limit - v_9;
								goto lab9_brk;
							}
							// ], line 110
							bra = cursor;
							// call R2, line 110
							if (!r_R2())
							{
								cursor = limit - v_9;
								goto lab9_brk;
							}
							switch (among_var)
							{
								
								case 0: 
									cursor = limit - v_9;
									goto lab9_brk;
								
								case 1: 
									// (, line 112
									// delete, line 112
									slice_del();
									break;
								}
						}
						while (false);

lab9_brk: ;
						
						break;
					}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			return true;
		}
		
		public override bool Stem()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			// (, line 122
			// do, line 123
			v_1 = cursor;
			do 
			{
				// call prelude, line 123
				if (!r_prelude())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 124
			v_2 = cursor;
			do 
			{
				// call mark_regions, line 124
				if (!r_mark_regions())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
			// backwards, line 125
			limit_backward = cursor; cursor = limit;
			// do, line 126
			v_3 = limit - cursor;
			do 
			{
				// call standard_suffix, line 126
				if (!r_standard_suffix())
				{
					goto lab2_brk;
				}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			cursor = limit_backward; // do, line 127
			v_4 = cursor;
			do 
			{
				// call postlude, line 127
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
