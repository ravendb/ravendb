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
	public class German2Stemmer : SnowballProgram
	{
		public German2Stemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("", - 1, 6, "", this), new Among("ae", 0, 2, "", this), new Among("oe", 0, 3, "", this), new Among("qu", 0, 5, "", this), new Among("ue", 0, 4, "", this), new Among("\u00DF", 0, 1, "", this)};
			a_1 = new Among[]{new Among("", - 1, 6, "", this), new Among("U", 0, 2, "", this), new Among("Y", 0, 1, "", this), new Among("\u00E4", 0, 3, "", this), new Among("\u00F6", 0, 4, "", this), new Among("\u00FC", 0, 5, "", this)};
			a_2 = new Among[]{new Among("e", - 1, 1, "", this), new Among("em", - 1, 1, "", this), new Among("en", - 1, 1, "", this), new Among("ern", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("s", - 1, 2, "", this), new Among("es", 5, 1, "", this)};
			a_3 = new Among[]{new Among("en", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("st", - 1, 2, "", this), new Among("est", 2, 1, "", this)};
			a_4 = new Among[]{new Among("ig", - 1, 1, "", this), new Among("lich", - 1, 1, "", this)};
			a_5 = new Among[]{new Among("end", - 1, 1, "", this), new Among("ig", - 1, 2, "", this), new Among("ung", - 1, 1, "", this), new Among("lich", - 1, 3, "", this), new Among("isch", - 1, 2, "", this), new Among("ik", - 1, 2, "", this), new Among("heit", - 1, 3, "", this), new Among("keit", - 1, 4, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private Among[] a_5;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (8), (char) (0), (char) (32), (char) (8)};
		private static readonly char[] g_s_ending = new char[]{(char) (117), (char) (30), (char) (5)};
		private static readonly char[] g_st_ending = new char[]{(char) (117), (char) (30), (char) (4)};
		
		private int I_p2;
		private int I_p1;

        protected internal virtual void  copy_from(German2Stemmer other)
		{
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
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
			// (, line 28
			// test, line 30
			v_1 = cursor;
			// repeat, line 30
			while (true)
			{
				v_2 = cursor;
				do 
				{
					// goto, line 30
					while (true)
					{
						v_3 = cursor;
						do 
						{
							// (, line 30
							if (!(in_grouping(g_v, 97, 252)))
							{
								goto lab3_brk;
							}
							// [, line 31
							bra = cursor;
							// or, line 31
							do 
							{
								v_4 = cursor;
								do 
								{
									// (, line 31
									// literal, line 31
									if (!(eq_s(1, "u")))
									{
										goto lab5_brk;
									}
									// ], line 31
									ket = cursor;
									if (!(in_grouping(g_v, 97, 252)))
									{
										goto lab5_brk;
									}
									// <-, line 31
									slice_from("U");
									goto lab4_brk;
								}
								while (false);

lab5_brk: ;
								
								cursor = v_4;
								// (, line 32
								// literal, line 32
								if (!(eq_s(1, "y")))
								{
									goto lab3_brk;
								}
								// ], line 32
								ket = cursor;
								if (!(in_grouping(g_v, 97, 252)))
								{
									goto lab3_brk;
								}
								// <-, line 32
								slice_from("Y");
							}
							while (false);

lab4_brk: ;
							
							cursor = v_3;
							goto golab2_brk;
						}
						while (false);

lab3_brk: ;
						
						cursor = v_3;
						if (cursor >= limit)
						{
							goto lab1_brk;
						}
						cursor++;
					}

golab2_brk: ;
					
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
			// repeat, line 35
			while (true)
			{
				v_5 = cursor;
				do 
				{
					// (, line 35
					// [, line 36
					bra = cursor;
					// substring, line 36
					among_var = find_among(a_0, 6);
					if (among_var == 0)
					{
						goto lab7_brk;
					}
					// ], line 36
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab7_brk;
						
						case 1: 
							// (, line 37
							// <-, line 37
							slice_from("ss");
							break;
						
						case 2: 
							// (, line 38
							// <-, line 38
							slice_from("\u00E4");
							break;
						
						case 3: 
							// (, line 39
							// <-, line 39
							slice_from("\u00F6");
							break;
						
						case 4: 
							// (, line 40
							// <-, line 40
							slice_from("\u00FC");
							break;
						
						case 5: 
							// (, line 41
							// hop, line 41
							{
								int c = cursor + 2;
								if (0 > c || c > limit)
								{
									goto lab7_brk;
								}
								cursor = c;
							}
							break;
						
						case 6: 
							// (, line 42
							// next, line 42
							if (cursor >= limit)
							{
								goto lab7_brk;
							}
							cursor++;
							break;
						}
					goto replab6;
				}
				while (false);

lab7_brk: ;
				
				cursor = v_5;
				goto replab6_brk;

replab6: ;
			}

replab6_brk: ;
			
			return true;
		}
		
		private bool r_mark_regions()
		{
			// (, line 48
			I_p1 = limit;
			I_p2 = limit;
			// gopast, line 53
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
			
			// gopast, line 53
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
			
			// setmark p1, line 53
			I_p1 = cursor;
			// try, line 54
			do 
			{
				// (, line 54
				if (!(I_p1 < 3))
				{
					goto lab4_brk;
				}
				I_p1 = 3;
			}
			while (false);

lab4_brk: ;
			
			// gopast, line 55
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
			
			// gopast, line 55
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
			
			// setmark p2, line 55
			I_p2 = cursor;
			return true;
		}
		
		private bool r_postlude()
		{
			int among_var;
			int v_1;
			// repeat, line 59
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 59
					// [, line 61
					bra = cursor;
					// substring, line 61
					among_var = find_among(a_1, 6);
					if (among_var == 0)
					{
						goto lab2_brk;
					}
					// ], line 61
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab2_brk;
						
						case 1: 
							// (, line 62
							// <-, line 62
							slice_from("y");
							break;
						
						case 2: 
							// (, line 63
							// <-, line 63
							slice_from("u");
							break;
						
						case 3: 
							// (, line 64
							// <-, line 64
							slice_from("a");
							break;
						
						case 4: 
							// (, line 65
							// <-, line 65
							slice_from("o");
							break;
						
						case 5: 
							// (, line 66
							// <-, line 66
							slice_from("u");
							break;
						
						case 6: 
							// (, line 67
							// next, line 67
							if (cursor >= limit)
							{
								goto lab2_brk;
							}
							cursor++;
							break;
						}
					goto replab1;
				}
				while (false);

lab2_brk: ;
				
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
			// (, line 77
			// do, line 78
			v_1 = limit - cursor;
			do 
			{
				// (, line 78
				// [, line 79
				ket = cursor;
				// substring, line 79
				among_var = find_among_b(a_2, 7);
				if (among_var == 0)
				{
					goto lab0_brk;
				}
				// ], line 79
				bra = cursor;
				// call R1, line 79
				if (!r_R1())
				{
					goto lab0_brk;
				}
				switch (among_var)
				{
					
					case 0: 
						goto lab0_brk;
					
					case 1: 
						// (, line 81
						// delete, line 81
						slice_del();
						break;
					
					case 2: 
						// (, line 84
						if (!(in_grouping_b(g_s_ending, 98, 116)))
						{
							goto lab0_brk;
						}
						// delete, line 84
						slice_del();
						break;
					}
			}
			while (false);

lab0_brk: ;
			
			cursor = limit - v_1;
			// do, line 88
			v_2 = limit - cursor;
			do 
			{
				// (, line 88
				// [, line 89
				ket = cursor;
				// substring, line 89
				among_var = find_among_b(a_3, 4);
				if (among_var == 0)
				{
					goto lab1_brk;
				}
				// ], line 89
				bra = cursor;
				// call R1, line 89
				if (!r_R1())
				{
					goto lab1_brk;
				}
				switch (among_var)
				{
					
					case 0: 
						goto lab1_brk;
					
					case 1: 
						// (, line 91
						// delete, line 91
						slice_del();
						break;
					
					case 2: 
						// (, line 94
						if (!(in_grouping_b(g_st_ending, 98, 116)))
						{
							goto lab1_brk;
						}
						// hop, line 94
						{
							int c = cursor - 3;
							if (limit_backward > c || c > limit)
							{
								goto lab1_brk;
							}
							cursor = c;
						}
						// delete, line 94
						slice_del();
						break;
					}
			}
			while (false);

lab1_brk: ;
			
			cursor = limit - v_2;
			// do, line 98
			v_3 = limit - cursor;
			do 
			{
				// (, line 98
				// [, line 99
				ket = cursor;
				// substring, line 99
				among_var = find_among_b(a_5, 8);
				if (among_var == 0)
				{
					goto lab2_brk;
				}
				// ], line 99
				bra = cursor;
				// call R2, line 99
				if (!r_R2())
				{
					goto lab2_brk;
				}
				switch (among_var)
				{
					
					case 0: 
						goto lab2_brk;
					
					case 1: 
						// (, line 101
						// delete, line 101
						slice_del();
						// try, line 102
						v_4 = limit - cursor;
						do 
						{
							// (, line 102
							// [, line 102
							ket = cursor;
							// literal, line 102
							if (!(eq_s_b(2, "ig")))
							{
								cursor = limit - v_4;
								goto lab3_brk;
							}
							// ], line 102
							bra = cursor;
							// not, line 102
							{
								v_5 = limit - cursor;
								do 
								{
									// literal, line 102
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
							// call R2, line 102
							if (!r_R2())
							{
								cursor = limit - v_4;
								goto lab3_brk;
							}
							// delete, line 102
							slice_del();
						}
						while (false);

lab3_brk: ;
						
						break;
					
					case 2: 
						// (, line 105
						// not, line 105
						{
							v_6 = limit - cursor;
							do 
							{
								// literal, line 105
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
						// delete, line 105
						slice_del();
						break;
					
					case 3: 
						// (, line 108
						// delete, line 108
						slice_del();
						// try, line 109
						v_7 = limit - cursor;
						do 
						{
							// (, line 109
							// [, line 110
							ket = cursor;
							// or, line 110
							do 
							{
								v_8 = limit - cursor;
								do 
								{
									// literal, line 110
									if (!(eq_s_b(2, "er")))
									{
										goto lab8_brk;
									}
									goto lab7_brk;
								}
								while (false);

lab8_brk: ;
								
								cursor = limit - v_8;
								// literal, line 110
								if (!(eq_s_b(2, "en")))
								{
									cursor = limit - v_7;
									goto lab6_brk;
								}
							}
							while (false);

lab7_brk: ;
							
							// ], line 110
							bra = cursor;
							// call R1, line 110
							if (!r_R1())
							{
								cursor = limit - v_7;
								goto lab6_brk;
							}
							// delete, line 110
							slice_del();
						}
						while (false);

lab6_brk: ;
						
						break;
					
					case 4: 
						// (, line 114
						// delete, line 114
						slice_del();
						// try, line 115
						v_9 = limit - cursor;
						do 
						{
							// (, line 115
							// [, line 116
							ket = cursor;
							// substring, line 116
							among_var = find_among_b(a_4, 2);
							if (among_var == 0)
							{
								cursor = limit - v_9;
								goto lab9_brk;
							}
							// ], line 116
							bra = cursor;
							// call R2, line 116
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
									// (, line 118
									// delete, line 118
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
			// (, line 128
			// do, line 129
			v_1 = cursor;
			do 
			{
				// call prelude, line 129
				if (!r_prelude())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 130
			v_2 = cursor;
			do 
			{
				// call mark_regions, line 130
				if (!r_mark_regions())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
			// backwards, line 131
			limit_backward = cursor; cursor = limit;
			// do, line 132
			v_3 = limit - cursor;
			do 
			{
				// call standard_suffix, line 132
				if (!r_standard_suffix())
				{
					goto lab2_brk;
				}
			}
			while (false);

lab2_brk: ;
			
			cursor = limit - v_3;
			cursor = limit_backward; // do, line 133
			v_4 = cursor;
			do 
			{
				// call postlude, line 133
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
