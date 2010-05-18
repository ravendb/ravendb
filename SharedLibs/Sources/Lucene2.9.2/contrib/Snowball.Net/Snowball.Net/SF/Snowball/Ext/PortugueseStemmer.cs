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
	
	/// <summary> Generated class implementing code defined by a snowball script.</summary>
	public class PortugueseStemmer : SnowballProgram
	{
		public PortugueseStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("", - 1, 3, "", this), new Among("\u00E3", 0, 1, "", this), new Among("\u00F5", 0, 2, "", this)};
			a_1 = new Among[]{new Among("", - 1, 3, "", this), new Among("a~", 0, 1, "", this), new Among("o~", 0, 2, "", this)};
			a_2 = new Among[]{new Among("ic", - 1, - 1, "", this), new Among("ad", - 1, - 1, "", this), new Among("os", - 1, - 1, "", this), new Among("iv", - 1, 1, "", this)};
			a_3 = new Among[]{new Among("avel", - 1, 1, "", this), new Among("\u00EDvel", - 1, 1, "", this)};
			a_4 = new Among[]{new Among("ic", - 1, 1, "", this), new Among("abil", - 1, 1, "", this), new Among("iv", - 1, 1, "", this)};
			a_5 = new Among[]{new Among("ica", - 1, 1, "", this), new Among("\u00EAncia", - 1, 4, "", this), new Among("ira", - 1, 9, "", this), new Among("adora", - 1, 1, "", this), new Among("osa", - 1, 1, "", this), new Among("ista", - 1, 1, "", this), new Among("iva", - 1, 8, "", this), new Among("eza", - 1, 1, "", this), new Among("log\u00EDa", - 1, 2, "", this), new Among("idade", - 1, 7, "", this), new Among("mente", - 1, 6, "", this), new Among("amente", 10, 5, "", this), new Among("\u00E1vel", - 1, 1, "", this), new Among("\u00EDvel", - 1, 1, "", this), new Among("uci\u00F3n", - 1, 3, "", this), new Among("ico", - 1, 1, "", this), new Among("ismo", - 1, 1, "", this), new Among("oso", - 1, 1, "", this), new Among("amento", - 1, 1, "", this), new Among("imento", - 1, 1, "", this), new Among("ivo", - 1, 8, "", this), new Among("a\u00E7a~o", - 1, 1, "", this), new Among("ador", - 1, 1, "", this), new Among("icas", - 1, 1, "", this), new Among("\u00EAncias", - 1, 4, "", this), new Among("iras", - 1, 9, "", this), new Among("adoras", - 1, 1, "", this), new Among("osas", - 1, 1, "", this), new Among("istas", - 1, 1, "", this), new Among("ivas", - 1, 8, "", this), new Among("ezas", - 1, 1, "", this), new Among("log\u00EDas", - 1, 2, "", this), new Among("idades", - 1, 7, "", this), new Among("uciones", - 1, 3, "", this), new Among("adores", - 1, 1, "", this), new Among("a\u00E7o~es", - 1, 1, "", this), new Among("icos", - 1, 1, "", this), new Among("ismos", - 1, 1, "", this), new Among("osos", - 1, 1, "", this), new Among("amentos", - 1, 1, "", this), new Among("imentos", - 1, 1, "", this), new Among("ivos", - 1, 8, "", this)};
			a_6 = new Among[]{new Among("ada", - 1, 1, "", this), new Among("ida", - 1, 1, "", this), new Among("ia", - 1, 1, "", this), new Among("aria", 2, 1, "", this), new Among("eria", 2, 1, "", this), new Among("iria", 2, 1, "", this), new Among("ara", - 1, 1, "", this), new Among("era", - 1, 1, "", this), new Among("ira", - 1, 1, "", this), new Among("ava", - 1, 1, "", this), new Among("asse", - 1, 1, "", this), new Among("esse", - 1, 1, "", this), new Among("isse", - 1, 1, "", this), new Among("aste", - 1, 1, "", this), new Among("este", - 1, 1, "", this), new Among("iste", - 1, 1, "", this), new Among("ei", - 1, 1, "", this), new Among("arei", 16, 1, "", this), new Among("erei", 16, 1, "", this), new Among("irei", 16, 1, "", this), new Among("am", - 1, 1, "", this), new Among("iam", 20, 1, "", this), new Among("ariam", 21, 1, "", this), new Among("eriam", 21, 1, "", this), new Among("iriam", 21, 1, "", this), new Among("aram", 20, 1, "", this), new Among("eram", 20, 1, "", this), new Among("iram", 20, 1, "", this), new Among("avam", 20, 1, "", this), new Among("em", - 1, 1, "", this), new Among("arem", 29, 1, "", this), new Among("erem", 29, 1, "", this), new Among("irem", 29, 1, "", this), new Among("assem", 29, 1, "", this), new Among("essem", 29, 1, "", this), new Among("issem", 29, 1, "", this), new Among("ado", - 1, 1, "", this), new Among("ido", - 1, 1, "", this), new Among("ando", - 1, 1, "", this), new Among("endo", - 1, 1, "", this), new Among("indo", - 1, 1, "", this), new Among("ara~o", - 1, 1, "", this), new Among("era~o", - 1, 1, "", this), new Among("ira~o", - 1, 1, "", this), new Among("ar", - 1, 1, "", this), new Among("er", - 1, 1, "", this), new Among("ir", - 1, 1, "", this), new Among("as", - 1, 1, "", this), new Among("adas", 47, 1, "", this), new Among("idas", 47, 1, "", this), new Among("ias", 47, 1, "", this), new Among("arias", 50, 1, "", this), new Among("erias", 50, 1, "", this), new Among("irias", 50, 1, "", this), new Among("aras", 47, 1, "", this), new Among("eras", 47, 
				1, "", this), new Among("iras", 47, 1, "", this), new Among("avas", 47, 1, "", this), new Among("es", - 1, 1, "", this), new Among("ardes", 58, 1, "", this), new Among("erdes", 58, 1, "", this), new Among("irdes", 58, 1, "", this), new Among("ares", 58, 1, "", this), new Among("eres", 58, 1, "", this), new Among("ires", 58, 1, "", this), new Among("asses", 58, 1, "", this), new Among("esses", 58, 1, "", this), new Among("isses", 58, 1, "", this), new Among("astes", 58, 1, "", this), new Among("estes", 58, 1, "", this), new Among("istes", 58, 1, "", this), new Among("is", - 1, 1, "", this), new Among("ais", 71, 1, "", this), new Among("eis", 71, 1, "", this), new Among("areis", 73, 1, "", this), new Among("ereis", 73, 1, "", this), new Among("ireis", 73, 1, "", this), new Among("\u00E1reis", 73, 1, "", this), new Among("\u00E9reis", 73, 1, "", this), new Among("\u00EDreis", 73, 1, "", this), new Among("\u00E1sseis", 73, 1, "", this), new Among("\u00E9sseis", 73, 1, "", this), new Among("\u00EDsseis", 73, 1, "", this), new Among("\u00E1veis", 73, 1, "", this), new Among("\u00EDeis", 73, 1, "", this), new Among("ar\u00EDeis", 84, 1, "", this), new Among("er\u00EDeis", 84, 1, "", this), new Among("ir\u00EDeis", 84, 1, "", this), new Among("ados", - 1, 1, "", this), new Among("idos", - 1, 1, "", this), new Among("amos", - 1, 1, "", this), new Among("\u00E1ramos", 90, 1, "", this), new Among("\u00E9ramos", 90, 1, "", this), new Among("\u00EDramos", 90, 1, "", this), new Among("\u00E1vamos", 90, 1, "", this), new Among("\u00EDamos", 90, 1, "", this), new Among("ar\u00EDamos", 95, 1, "", this), new Among("er\u00EDamos", 95, 1, "", this), new Among("ir\u00EDamos", 95, 1, "", this), new Among("emos", - 1, 1, "", this), new Among("aremos", 99, 1, "", this), new Among("eremos", 99, 1, "", this), new Among("iremos", 99, 1, "", this), new Among("\u00E1ssemos", 99, 1, "", this), new Among("\u00EAssemos", 99, 1, "", this), new Among("\u00EDssemos", 99, 1, "", this), new Among("imos", - 1, 1, "", this), new 
				Among("armos", - 1, 1, "", this), new Among("ermos", - 1, 1, "", this), new Among("irmos", - 1, 1, "", this), new Among("\u00E1mos", - 1, 1, "", this), new Among("ar\u00E1s", - 1, 1, "", this), new Among("er\u00E1s", - 1, 1, "", this), new Among("ir\u00E1s", - 1, 1, "", this), new Among("eu", - 1, 1, "", this), new Among("iu", - 1, 1, "", this), new Among("ou", - 1, 1, "", this), new Among("ar\u00E1", - 1, 1, "", this), new Among("er\u00E1", - 1, 1, "", this), new Among("ir\u00E1", - 1, 1, "", this)};
			a_7 = new Among[]{new Among("a", - 1, 1, "", this), new Among("i", - 1, 1, "", this), new Among("o", - 1, 1, "", this), new Among("os", - 1, 1, "", this), new Among("\u00E1", - 1, 1, "", this), new Among("\u00ED", - 1, 1, "", this), new Among("\u00F3", - 1, 1, "", this)};
			a_8 = new Among[]{new Among("e", - 1, 1, "", this), new Among("\u00E7", - 1, 2, "", this), new Among("\u00E9", - 1, 1, "", this), new Among("\u00EA", - 1, 1, "", this)};
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
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (3), (char) (19), (char) (12), (char) (2)};
		
		private int I_p2;
		private int I_p1;
		private int I_pV;
		
		protected internal virtual void  copy_from(PortugueseStemmer other)
		{
			I_p2 = other.I_p2;
			I_p1 = other.I_p1;
			I_pV = other.I_pV;
			base.copy_from(other);
		}
		
		private bool r_prelude()
		{
			int among_var;
			int v_1;
			// repeat, line 36
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 36
					// [, line 37
					bra = cursor;
					// substring, line 37
					among_var = find_among(a_0, 3);
					if (among_var == 0)
					{
						goto lab1_brk;
					}
					// ], line 37
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab1_brk;
						
						case 1: 
							// (, line 38
							// <-, line 38
							slice_from("a~");
							break;
						
						case 2: 
							// (, line 39
							// <-, line 39
							slice_from("o~");
							break;
						
						case 3: 
							// (, line 40
							// next, line 40
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
				
				cursor = v_1;
				goto replab0_brk;

replab0: ;
			}

replab0_brk: ;
			
			return true;
		}
		
		private bool r_mark_regions()
		{
			int v_1;
			int v_2;
			int v_3;
			int v_6;
			int v_8;
			// (, line 44
			I_pV = limit;
			I_p1 = limit;
			I_p2 = limit;
			// do, line 50
			v_1 = cursor;
			do 
			{
				// (, line 50
				// or, line 52
lab2: 
				do 
				{
					v_2 = cursor;
					do 
					{
						// (, line 51
						if (!(in_grouping(g_v, 97, 250)))
						{
							goto lab2_brk;
						}
						// or, line 51
						do 
						{
							v_3 = cursor;
							do 
							{
								// (, line 51
								if (!(out_grouping(g_v, 97, 250)))
								{
									goto lab4_brk;
								}
								// gopast, line 51
								while (true)
								{
									do 
									{
										if (!(in_grouping(g_v, 97, 250)))
										{
											goto lab6_brk;
										}
										goto golab5_brk;
									}
									while (false);

lab6_brk: ;
									
									if (cursor >= limit)
									{
										goto lab4_brk;
									}
									cursor++;
								}

golab5_brk: ;
								
								goto lab3_brk;
							}
							while (false);

lab4_brk: ;
							
							cursor = v_3;
							// (, line 51
							if (!(in_grouping(g_v, 97, 250)))
							{
								goto lab2_brk;
							}
							// gopast, line 51
							while (true)
							{
								do 
								{
									if (!(out_grouping(g_v, 97, 250)))
									{
										goto lab8_brk;
									}
									goto golab7_brk;
								}
								while (false);

lab8_brk: ;
								
								if (cursor >= limit)
								{
									goto lab2_brk;
								}
								cursor++;
							}

golab7_brk: ;
							
						}
						while (false);

lab3_brk: ;
						
						goto lab2_brk;
					}
					while (false);

lab2_brk: ;
					
					cursor = v_2;
					// (, line 53
					if (!(out_grouping(g_v, 97, 250)))
					{
						goto lab0_brk;
					}
					// or, line 53
					do 
					{
						v_6 = cursor;
						do 
						{
							// (, line 53
							if (!(out_grouping(g_v, 97, 250)))
							{
								goto lab10_brk;
							}
							// gopast, line 53
							while (true)
							{
								do 
								{
									if (!(in_grouping(g_v, 97, 250)))
									{
										goto lab12_brk;
									}
									goto golab11_brk;
								}
								while (false);

lab12_brk: ;
								
								if (cursor >= limit)
								{
									goto lab10_brk;
								}
								cursor++;
							}

golab11_brk: ;
							
							goto lab9_brk;
						}
						while (false);

lab10_brk: ;
						
						cursor = v_6;
						// (, line 53
						if (!(in_grouping(g_v, 97, 250)))
						{
							goto lab0_brk;
						}
						// next, line 53
						if (cursor >= limit)
						{
							goto lab0_brk;
						}
						cursor++;
					}
					while (false);

lab9_brk: ;
					
				}
				while (false);
				// setmark pV, line 54
				I_pV = cursor;
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 56
			v_8 = cursor;
			do 
			{
				// (, line 56
				// gopast, line 57
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 250)))
						{
							goto lab15_brk;
						}
						goto golab14_brk;
					}
					while (false);

lab15_brk: ;
					
					if (cursor >= limit)
					{
						goto lab13_brk;
					}
					cursor++;
				}

golab14_brk: ;
				
				// gopast, line 57
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 250)))
						{
							goto lab17_brk;
						}
						goto golab16_brk;
					}
					while (false);

lab17_brk: ;
					
					if (cursor >= limit)
					{
						goto lab13_brk;
					}
					cursor++;
				}

golab16_brk: ;
				
				// setmark p1, line 57
				I_p1 = cursor;
				// gopast, line 58
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 250)))
						{
							goto lab19_brk;
						}
						goto golab18_brk;
					}
					while (false);

lab19_brk: ;
					
					if (cursor >= limit)
					{
						goto lab13_brk;
					}
					cursor++;
				}

golab18_brk: ;
				
				// gopast, line 58
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 250)))
						{
							goto lab21_brk;
						}
						goto golab20_brk;
					}
					while (false);

lab21_brk: ;
					
					if (cursor >= limit)
					{
						goto lab13_brk;
					}
					cursor++;
				}

golab20_brk: ;
				
				// setmark p2, line 58
				I_p2 = cursor;
			}
			while (false);

lab13_brk: ;
			
			cursor = v_8;
			return true;
		}
		
		private bool r_postlude()
		{
			int among_var;
			int v_1;
			// repeat, line 62
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 62
					// [, line 63
					bra = cursor;
					// substring, line 63
					among_var = find_among(a_1, 3);
					if (among_var == 0)
					{
						goto lab5_brk;
					}
					// ], line 63
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab5_brk;
						
						case 1: 
							// (, line 64
							// <-, line 64
							slice_from("\u00E3");
							break;
						
						case 2: 
							// (, line 65
							// <-, line 65
							slice_from("\u00F5");
							break;
						
						case 3: 
							// (, line 66
							// next, line 66
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
		
		private bool r_standard_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			// (, line 76
			// [, line 77
			ket = cursor;
			// substring, line 77
			among_var = find_among_b(a_5, 42);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 77
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 91
					// call R2, line 92
					if (!r_R2())
					{
						return false;
					}
					// delete, line 92
					slice_del();
					break;
				
				case 2: 
					// (, line 96
					// call R2, line 97
					if (!r_R2())
					{
						return false;
					}
					// <-, line 97
					slice_from("log");
					break;
				
				case 3: 
					// (, line 100
					// call R2, line 101
					if (!r_R2())
					{
						return false;
					}
					// <-, line 101
					slice_from("u");
					break;
				
				case 4: 
					// (, line 104
					// call R2, line 105
					if (!r_R2())
					{
						return false;
					}
					// <-, line 105
					slice_from("ente");
					break;
				
				case 5: 
					// (, line 108
					// call R1, line 109
					if (!r_R1())
					{
						return false;
					}
					// delete, line 109
					slice_del();
					// try, line 110
					v_1 = limit - cursor;
					do 
					{
						// (, line 110
						// [, line 111
						ket = cursor;
						// substring, line 111
						among_var = find_among_b(a_2, 4);
						if (among_var == 0)
						{
							cursor = limit - v_1;
							goto lab0_brk;
						}
						// ], line 111
						bra = cursor;
						// call R2, line 111
						if (!r_R2())
						{
							cursor = limit - v_1;
							goto lab0_brk;
						}
						// delete, line 111
						slice_del();
						switch (among_var)
						{
							
							case 0: 
								cursor = limit - v_1;
								goto lab0_brk;
							
							case 1: 
								// (, line 112
								// [, line 112
								ket = cursor;
								// literal, line 112
								if (!(eq_s_b(2, "at")))
								{
									cursor = limit - v_1;
									goto lab0_brk;
								}
								// ], line 112
								bra = cursor;
								// call R2, line 112
								if (!r_R2())
								{
									cursor = limit - v_1;
									goto lab0_brk;
								}
								// delete, line 112
								slice_del();
								break;
							}
					}
					while (false);

lab0_brk: ;
					
					break;
				
				case 6: 
					// (, line 120
					// call R2, line 121
					if (!r_R2())
					{
						return false;
					}
					// delete, line 121
					slice_del();
					// try, line 122
					v_2 = limit - cursor;
					do 
					{
						// (, line 122
						// [, line 123
						ket = cursor;
						// substring, line 123
						among_var = find_among_b(a_3, 2);
						if (among_var == 0)
						{
							cursor = limit - v_2;
							goto lab1_brk;
						}
						// ], line 123
						bra = cursor;
						switch (among_var)
						{
							
							case 0: 
								cursor = limit - v_2;
								goto lab1_brk;
							
							case 1: 
								// (, line 125
								// call R2, line 125
								if (!r_R2())
								{
									cursor = limit - v_2;
									goto lab1_brk;
								}
								// delete, line 125
								slice_del();
								break;
							}
					}
					while (false);

lab1_brk: ;
					
					break;
				
				case 7: 
					// (, line 131
					// call R2, line 132
					if (!r_R2())
					{
						return false;
					}
					// delete, line 132
					slice_del();
					// try, line 133
					v_3 = limit - cursor;
					do 
					{
						// (, line 133
						// [, line 134
						ket = cursor;
						// substring, line 134
						among_var = find_among_b(a_4, 3);
						if (among_var == 0)
						{
							cursor = limit - v_3;
							goto lab2_brk;
						}
						// ], line 134
						bra = cursor;
						switch (among_var)
						{
							
							case 0: 
								cursor = limit - v_3;
								goto lab2_brk;
							
							case 1: 
								// (, line 137
								// call R2, line 137
								if (!r_R2())
								{
									cursor = limit - v_3;
									goto lab2_brk;
								}
								// delete, line 137
								slice_del();
								break;
							}
					}
					while (false);

lab2_brk: ;
					
					break;
				
				case 8: 
					// (, line 143
					// call R2, line 144
					if (!r_R2())
					{
						return false;
					}
					// delete, line 144
					slice_del();
					// try, line 145
					v_4 = limit - cursor;
					do 
					{
						// (, line 145
						// [, line 146
						ket = cursor;
						// literal, line 146
						if (!(eq_s_b(2, "at")))
						{
							cursor = limit - v_4;
							goto lab3_brk;
						}
						// ], line 146
						bra = cursor;
						// call R2, line 146
						if (!r_R2())
						{
							cursor = limit - v_4;
							goto lab3_brk;
						}
						// delete, line 146
						slice_del();
					}
					while (false);

lab3_brk: ;
					
					break;
				
				case 9: 
					// (, line 150
					// call RV, line 151
					if (!r_RV())
					{
						return false;
					}
					// literal, line 151
					if (!(eq_s_b(1, "e")))
					{
						return false;
					}
					// <-, line 152
					slice_from("ir");
					break;
				}
			return true;
		}
		
		private bool r_verb_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			// setlimit, line 157
			v_1 = limit - cursor;
			// tomark, line 157
			if (cursor < I_pV)
			{
				return false;
			}
			cursor = I_pV;
			v_2 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_1;
			// (, line 157
			// [, line 158
			ket = cursor;
			// substring, line 158
			among_var = find_among_b(a_6, 120);
			if (among_var == 0)
			{
				limit_backward = v_2;
				return false;
			}
			// ], line 158
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					limit_backward = v_2;
					return false;
				
				case 1: 
					// (, line 177
					// delete, line 177
					slice_del();
					break;
				}
			limit_backward = v_2;
			return true;
		}
		
		private bool r_residual_suffix()
		{
			int among_var;
			// (, line 181
			// [, line 182
			ket = cursor;
			// substring, line 182
			among_var = find_among_b(a_7, 7);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 182
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 185
					// call RV, line 185
					if (!r_RV())
					{
						return false;
					}
					// delete, line 185
					slice_del();
					break;
				}
			return true;
		}
		
		private bool r_residual_form()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			// (, line 189
			// [, line 190
			ket = cursor;
			// substring, line 190
			among_var = find_among_b(a_8, 4);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 190
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 192
					// call RV, line 192
					if (!r_RV())
					{
						return false;
					}
					// delete, line 192
					slice_del();
					// [, line 192
					ket = cursor;
					// or, line 192
lab5: 
					do 
					{
						v_1 = limit - cursor;
						do 
						{
							// (, line 192
							// literal, line 192
							if (!(eq_s_b(1, "u")))
							{
								goto lab5_brk;
							}
							// ], line 192
							bra = cursor;
							// test, line 192
							v_2 = limit - cursor;
							// literal, line 192
							if (!(eq_s_b(1, "g")))
							{
								goto lab5_brk;
							}
							cursor = limit - v_2;
							goto lab5_brk;
						}
						while (false);

lab5_brk: ;
						
						cursor = limit - v_1;
						// (, line 193
						// literal, line 193
						if (!(eq_s_b(1, "i")))
						{
							return false;
						}
						// ], line 193
						bra = cursor;
						// test, line 193
						v_3 = limit - cursor;
						// literal, line 193
						if (!(eq_s_b(1, "c")))
						{
							return false;
						}
						cursor = limit - v_3;
					}
					while (false);
					// call RV, line 193
					if (!r_RV())
					{
						return false;
					}
					// delete, line 193
					slice_del();
					break;
				
				case 2: 
					// (, line 194
					// <-, line 194
					slice_from("c");
					break;
				}
			return true;
		}
		
		public virtual bool Stem()
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
			// (, line 199
			// do, line 200
			v_1 = cursor;
			do 
			{
				// call prelude, line 200
				if (!r_prelude())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 201
			v_2 = cursor;
			do 
			{
				// call mark_regions, line 201
				if (!r_mark_regions())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
			// backwards, line 202
			limit_backward = cursor; cursor = limit;
			// (, line 202
			// do, line 203
			v_3 = limit - cursor;			do 
			{
				// (, line 203
				// or, line 207
				do 
				{
					v_4 = limit - cursor;
					do 
					{
						// (, line 204
						// and, line 205
						v_5 = limit - cursor;
						// (, line 204
						// or, line 204
						do 
						{
							v_6 = limit - cursor;
							do 
							{
								// call standard_suffix, line 204
								if (!r_standard_suffix())
								{
									goto lab6_brk;
								}
								goto lab5_brk;
							}
							while (false);

lab6_brk: ;
							
							cursor = limit - v_6;
							// call verb_suffix, line 204
							if (!r_verb_suffix())
							{
								goto lab4_brk;
							}
						}
						while (false);

lab5_brk: ;
						
						cursor = limit - v_5;
						// do, line 205
						v_7 = limit - cursor;
						do 
						{
							// (, line 205
							// [, line 205
							ket = cursor;
							// literal, line 205
							if (!(eq_s_b(1, "i")))
							{
								goto lab7_brk;
							}
							// ], line 205
							bra = cursor;
							// test, line 205
							v_8 = limit - cursor;
							// literal, line 205
							if (!(eq_s_b(1, "c")))
							{
								goto lab7_brk;
							}
							cursor = limit - v_8;
							// call RV, line 205
							if (!r_RV())
							{
								goto lab7_brk;
							}
							// delete, line 205
							slice_del();
						}
						while (false);

lab7_brk: ;
						
						cursor = limit - v_7;
						goto lab5_brk;
					}
					while (false);

lab4_brk: ;
					
					cursor = limit - v_4;
					// call residual_suffix, line 207
					if (!r_residual_suffix())
					{
						goto lab2_brk;
					}
				}
				while (false);

lab3_brk: ;
				
			}
			while (false);

lab2_brk: ;

			cursor = limit - v_3;
			// do, line 209
			v_9 = limit - cursor;
			do 
			{
				// call residual_form, line 209
				if (!r_residual_form())
				{
					goto lab8_brk;
				}
			}
			while (false);

lab8_brk: ;
			
			cursor = limit - v_9;
			cursor = limit_backward; // do, line 211
			v_10 = cursor;
			do 
			{
				// call postlude, line 211
				if (!r_postlude())
				{
					goto lab9_brk;
				}
			}
			while (false);

lab9_brk: ;
			
			cursor = v_10;
			return true;
		}
	}
}
