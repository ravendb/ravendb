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
	public class FrenchStemmer : SnowballProgram
	{
		public FrenchStemmer()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			a_0 = new Among[]{new Among("", - 1, 4, "", this), new Among("I", 0, 1, "", this), new Among("U", 0, 2, "", this), new Among("Y", 0, 3, "", this)};
			a_1 = new Among[]{new Among("iqU", - 1, 3, "", this), new Among("abl", - 1, 3, "", this), new Among("I\u00E8r", - 1, 4, "", this), new Among("i\u00E8r", - 1, 4, "", this), new Among("eus", - 1, 2, "", this), new Among("iv", - 1, 1, "", this)};
			a_2 = new Among[]{new Among("ic", - 1, 2, "", this), new Among("abil", - 1, 1, "", this), new Among("iv", - 1, 3, "", this)};
			a_3 = new Among[]{new Among("iqUe", - 1, 1, "", this), new Among("atrice", - 1, 2, "", this), new Among("ance", - 1, 1, "", this), new Among("ence", - 1, 5, "", this), new Among("logie", - 1, 3, "", this), new Among("able", - 1, 1, "", this), new Among("isme", - 1, 1, "", this), new Among("euse", - 1, 11, "", this), new Among("iste", - 1, 1, "", this), new Among("ive", - 1, 8, "", this), new Among("if", - 1, 8, "", this), new Among("usion", - 1, 4, "", this), new Among("ation", - 1, 2, "", this), new Among("ution", - 1, 4, "", this), new Among("ateur", - 1, 2, "", this), new Among("iqUes", - 1, 1, "", this), new Among("atrices", - 1, 2, "", this), new Among("ances", - 1, 1, "", this), new Among("ences", - 1, 5, "", this), new Among("logies", - 1, 3, "", this), new Among("ables", - 1, 1, "", this), new Among("ismes", - 1, 1, "", this), new Among("euses", - 1, 11, "", this), new Among("istes", - 1, 1, "", this), new Among("ives", - 1, 8, "", this), new Among("ifs", - 1, 8, "", this), new Among("usions", - 1, 4, "", this), new Among("ations", - 1, 2, "", this), new Among("utions", - 1, 4, "", this), new Among("ateurs", - 1, 2, "", this), new Among("ments", - 1, 15, "", this), new Among("ements", 30, 6, "", this), new Among("issements", 31, 12, "", this), new Among("it\u00E9s", - 1, 7, "", this), new Among("ment", - 1, 15, "", this), new Among("ement", 34, 6, "", this), new Among("issement", 35, 12, "", this), new Among("amment", 34, 13, "", this), new Among("emment", 34, 14, "", this), new Among("aux", - 1, 10, "", this), new Among("eaux", 39, 9, "", this), new Among("eux", - 1, 1, "", this), new Among("it\u00E9", - 1, 7, "", this)};
			a_4 = new Among[]{new Among("ira", - 1, 1, "", this), new Among("ie", - 1, 1, "", this), new Among("isse", - 1, 1, "", this), new Among("issante", - 1, 1, "", this), new Among("i", - 1, 1, "", this), new Among("irai", 4, 1, "", this), new Among("ir", - 1, 1, "", this), new Among("iras", - 1, 1, "", this), new Among("ies", - 1, 1, "", this), new Among("\u00EEmes", - 1, 1, "", this), new Among("isses", - 1, 1, "", this), new Among("issantes", - 1, 1, "", this), new Among("\u00EEtes", - 1, 1, "", this), new Among("is", - 1, 1, "", this), new Among("irais", 13, 1, "", this), new Among("issais", 13, 1, "", this), new Among("irions", - 1, 1, "", this), new Among("issions", - 1, 1, "", this), new Among("irons", - 1, 1, "", this), new Among("issons", - 1, 1, "", this), new Among("issants", - 1, 1, "", this), new Among("it", - 1, 1, "", this), new Among("irait", 21, 1, "", this), new Among("issait", 21, 1, "", this), new Among("issant", - 1, 1, "", this), new Among("iraIent", - 1, 1, "", this), new Among("issaIent", - 1, 1, "", this), new Among("irent", - 1, 1, "", this), new Among("issent", - 1, 1, "", this), new Among("iront", - 1, 1, "", this), new Among("\u00EEt", - 1, 1, "", this), new Among("iriez", - 1, 1, "", this), new Among("issiez", - 1, 1, "", this), new Among("irez", - 1, 1, "", this), new Among("issez", - 1, 1, "", this)};
			a_5 = new Among[]{new Among("a", - 1, 3, "", this), new Among("era", 0, 2, "", this), new Among("asse", - 1, 3, "", this), new Among("ante", - 1, 3, "", this), new Among("\u00E9e", - 1, 2, "", this), new Among("ai", - 1, 3, "", this), new Among("erai", 5, 2, "", this), new Among("er", - 1, 2, "", this), new Among("as", - 1, 3, "", this), new Among("eras", 8, 2, "", this), new Among("\u00E2mes", - 1, 3, "", this), new Among("asses", - 1, 3, "", this), new Among("antes", - 1, 3, "", this), new Among("\u00E2tes", - 1, 3, "", this), new Among("\u00E9es", - 1, 2, "", this), new Among("ais", - 1, 3, "", this), new Among("erais", 15, 2, "", this), new Among("ions", - 1, 1, "", this), new Among("erions", 17, 2, "", this), new Among("assions", 17, 3, "", this), new Among("erons", - 1, 2, "", this), new Among("ants", - 1, 3, "", this), new Among("\u00E9s", - 1, 2, "", this), new Among("ait", - 1, 3, "", this), new Among("erait", 23, 2, "", this), new Among("ant", - 1, 3, "", this), new Among("aIent", - 1, 3, "", this), new Among("eraIent", 26, 2, "", this), new Among("\u00E8rent", - 1, 2, "", this), new Among("assent", - 1, 3, "", this), new Among("eront", - 1, 2, "", this), new Among("\u00E2t", - 1, 3, "", this), new Among("ez", - 1, 2, "", this), new Among("iez", 32, 2, "", this), new Among("eriez", 33, 2, "", this), new Among("assiez", 33, 3, "", this), new Among("erez", 32, 2, "", this), new Among("\u00E9", - 1, 2, "", this)};
			a_6 = new Among[]{new Among("e", - 1, 3, "", this), new Among("I\u00E8re", 0, 2, "", this), new Among("i\u00E8re", 0, 2, "", this), new Among("ion", - 1, 1, "", this), new Among("Ier", - 1, 2, "", this), new Among("ier", - 1, 2, "", this), new Among("\u00EB", - 1, 4, "", this)};
			a_7 = new Among[]{new Among("ell", - 1, - 1, "", this), new Among("eill", - 1, - 1, "", this), new Among("enn", - 1, - 1, "", this), new Among("onn", - 1, - 1, "", this), new Among("ett", - 1, - 1, "", this)};
		}
		
		private Among[] a_0;
		private Among[] a_1;
		private Among[] a_2;
		private Among[] a_3;
		private Among[] a_4;
		private Among[] a_5;
		private Among[] a_6;
		private Among[] a_7;
		private static readonly char[] g_v = new char[]{(char) (17), (char) (65), (char) (16), (char) (1), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (128), (char) (130), (char) (103), (char) (8), (char) (5)};
		private static readonly char[] g_keep_with_s = new char[]{(char) (1), (char) (65), (char) (20), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (0), (char) (128)};
		
		private int I_p2;
		private int I_p1;
		private int I_pV;
		protected internal virtual void  copy_from(FrenchStemmer other)
		{
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
			int v_4;
			// repeat, line 38
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// goto, line 38
					while (true)
					{
						v_2 = cursor;
						do 
						{
							// (, line 38
							// or, line 44
							do 
							{
								v_3 = cursor;
								do 
								{
									// (, line 40
									if (!(in_grouping(g_v, 97, 251)))
									{
										goto lab5_brk;
									}
									// [, line 40
									bra = cursor;
									// or, line 40
									do 
									{
										v_4 = cursor;
										do 
										{
											// (, line 40
											// literal, line 40
											if (!(eq_s(1, "u")))
											{
												goto lab7_brk;
											}
											// ], line 40
											ket = cursor;
											if (!(in_grouping(g_v, 97, 251)))
											{
												goto lab7_brk;
											}
											// <-, line 40
											slice_from("U");
											goto lab6_brk;
										}
										while (false);

lab7_brk: ;
										
										cursor = v_4;
										do 
										{
											// (, line 41
											// literal, line 41
											if (!(eq_s(1, "i")))
											{
												goto lab8_brk;
											}
											// ], line 41
											ket = cursor;
											if (!(in_grouping(g_v, 97, 251)))
											{
												goto lab8_brk;
											}
											// <-, line 41
											slice_from("I");
											goto lab6_brk;
										}
										while (false);

lab8_brk: ;
										
										cursor = v_4;
										// (, line 42
										// literal, line 42
										if (!(eq_s(1, "y")))
										{
											goto lab5_brk;
										}
										// ], line 42
										ket = cursor;
										// <-, line 42
										slice_from("Y");
									}
									while (false);

lab6_brk: ;
									
									goto lab4_brk;
								}
								while (false);

lab5_brk: ;
								
								cursor = v_3;
								do 
								{
									// (, line 45
									// [, line 45
									bra = cursor;
									// literal, line 45
									if (!(eq_s(1, "y")))
									{
										goto lab9_brk;
									}
									// ], line 45
									ket = cursor;
									if (!(in_grouping(g_v, 97, 251)))
									{
										goto lab9_brk;
									}
									// <-, line 45
									slice_from("Y");
									goto lab4_brk;
								}
								while (false);

lab9_brk: ;
								
								cursor = v_3;
								// (, line 47
								// literal, line 47
								if (!(eq_s(1, "q")))
								{
									goto lab3_brk;
								}
								// [, line 47
								bra = cursor;
								// literal, line 47
								if (!(eq_s(1, "u")))
								{
									goto lab3_brk;
								}
								// ], line 47
								ket = cursor;
								// <-, line 47
								slice_from("U");
							}
							while (false);

lab4_brk: ;
							
							cursor = v_2;
							goto golab2_brk;
						}
						while (false);

lab3_brk: ;
						
						cursor = v_2;
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
			int v_4;
			// (, line 50
			I_pV = limit;
			I_p1 = limit;
			I_p2 = limit;
			// do, line 56
			v_1 = cursor;
			do 
			{
				// (, line 56
				// or, line 57
				do 
				{
					v_2 = cursor;
					do 
					{
						// (, line 57
						if (!(in_grouping(g_v, 97, 251)))
						{
							goto lab2_brk;
						}
						if (!(in_grouping(g_v, 97, 251)))
						{
							goto lab2_brk;
						}
						// next, line 57
						if (cursor >= limit)
						{
							goto lab2_brk;
						}
						cursor++;
						goto lab1_brk;
					}
					while (false);

lab2_brk: ;
					
					cursor = v_2;
					// (, line 57
					// next, line 57
					if (cursor >= limit)
					{
						goto lab0_brk;
					}
					cursor++;
					// gopast, line 57
					while (true)
					{
						do 
						{
							if (!(in_grouping(g_v, 97, 251)))
							{
								goto lab4_brk;
							}
							goto golab3_brk;
						}
						while (false);

lab4_brk: ;
						
						if (cursor >= limit)
						{
							goto lab0_brk;
						}
						cursor++;
					}

golab3_brk: ;
					
				}
				while (false);

lab1_brk: ;
				// setmark pV, line 58
				I_pV = cursor;
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 60
			v_4 = cursor;
			do 
			{
				// (, line 60
				// gopast, line 61
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 251)))
						{
							goto lab7_brk;
						}
						goto golab6_brk;
					}
					while (false);

lab7_brk: ;
					
					if (cursor >= limit)
					{
						goto lab5_brk;
					}
					cursor++;
				}

golab6_brk: ;
				
				// gopast, line 61
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 251)))
						{
							goto lab9_brk;
						}
						goto golab8_brk;
					}
					while (false);

lab9_brk: ;
					
					if (cursor >= limit)
					{
						goto lab5_brk;
					}
					cursor++;
				}

golab8_brk: ;
				
				// setmark p1, line 61
				I_p1 = cursor;
				// gopast, line 62
				while (true)
				{
					do 
					{
						if (!(in_grouping(g_v, 97, 251)))
						{
							goto lab11_brk;
						}
						goto golab10_brk;
					}
					while (false);

lab11_brk: ;
					
					if (cursor >= limit)
					{
						goto lab5_brk;
					}
					cursor++;
				}

golab10_brk: ;
				
				// gopast, line 62
				while (true)
				{
					do 
					{
						if (!(out_grouping(g_v, 97, 251)))
						{
							goto lab13_brk;
						}
						goto golab12_brk;
					}
					while (false);

lab13_brk: ;
					
					if (cursor >= limit)
					{
						goto lab5_brk;
					}
					cursor++;
				}

golab12_brk: ;
				
				// setmark p2, line 62
				I_p2 = cursor;
			}
			while (false);

lab5_brk: ;
			
			cursor = v_4;
			return true;
		}
		
		private bool r_postlude()
		{
			int among_var;
			int v_1;
			// repeat, line 66
			while (true)
			{
				v_1 = cursor;
				do 
				{
					// (, line 66
					// [, line 68
					bra = cursor;
					// substring, line 68
					among_var = find_among(a_0, 4);
					if (among_var == 0)
					{
						goto lab10_brk;
					}
					// ], line 68
					ket = cursor;
					switch (among_var)
					{
						
						case 0: 
							goto lab10_brk;
						
						case 1: 
							// (, line 69
							// <-, line 69
							slice_from("i");
							break;
						
						case 2: 
							// (, line 70
							// <-, line 70
							slice_from("u");
							break;
						
						case 3: 
							// (, line 71
							// <-, line 71
							slice_from("y");
							break;
						
						case 4: 
							// (, line 72
							// next, line 72
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
			int v_5;
			int v_6;
			int v_7;
			int v_8;
			int v_9;
			int v_10;
			int v_11;
			// (, line 82
			// [, line 83
			ket = cursor;
			// substring, line 83
			among_var = find_among_b(a_3, 43);
			if (among_var == 0)
			{
				return false;
			}
			// ], line 83
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					return false;
				
				case 1: 
					// (, line 87
					// call R2, line 87
					if (!r_R2())
					{
						return false;
					}
					// delete, line 87
					slice_del();
					break;
				
				case 2: 
					// (, line 90
					// call R2, line 90
					if (!r_R2())
					{
						return false;
					}
					// delete, line 90
					slice_del();
					// try, line 91
					v_1 = limit - cursor;
					do 
					{
						// (, line 91
						// [, line 91
						ket = cursor;
						// literal, line 91
						if (!(eq_s_b(2, "ic")))
						{
							cursor = limit - v_1;
							goto lab0_brk;
						}
						// ], line 91
						bra = cursor;
						// or, line 91
						do 
						{
							v_2 = limit - cursor;
							do 
							{
								// (, line 91
								// call R2, line 91
								if (!r_R2())
								{
									goto lab2_brk;
								}
								// delete, line 91
								slice_del();
								goto lab1_brk;
							}
							while (false);

lab2_brk: ;
							
							cursor = limit - v_2;
							// <-, line 91
							slice_from("iqU");
						}
						while (false);

lab1_brk: ;
					}
					while (false);

lab0_brk: ;
					
					break;
				
				case 3: 
					// (, line 95
					// call R2, line 95
					if (!r_R2())
					{
						return false;
					}
					// <-, line 95
					slice_from("log");
					break;
				
				case 4: 
					// (, line 98
					// call R2, line 98
					if (!r_R2())
					{
						return false;
					}
					// <-, line 98
					slice_from("u");
					break;
				
				case 5: 
					// (, line 101
					// call R2, line 101
					if (!r_R2())
					{
						return false;
					}
					// <-, line 101
					slice_from("ent");
					break;
				
				case 6: 
					// (, line 104
					// call RV, line 105
					if (!r_RV())
					{
						return false;
					}
					// delete, line 105
					slice_del();
					// try, line 106
					v_3 = limit - cursor;
					do 
					{
						// (, line 106
						// [, line 107
						ket = cursor;
						// substring, line 107
						among_var = find_among_b(a_1, 6);
						if (among_var == 0)
						{
							cursor = limit - v_3;
							goto lab3_brk;
						}
						// ], line 107
						bra = cursor;
						switch (among_var)
						{
							
							case 0: 
								cursor = limit - v_3;
								goto lab3_brk;
							
							case 1: 
								// (, line 108
								// call R2, line 108
								if (!r_R2())
								{
									cursor = limit - v_3;
									goto lab3_brk;
								}
								// delete, line 108
								slice_del();
								// [, line 108
								ket = cursor;
								// literal, line 108
								if (!(eq_s_b(2, "at")))
								{
									cursor = limit - v_3;
									goto lab3_brk;
								}
								// ], line 108
								bra = cursor;
								// call R2, line 108
								if (!r_R2())
								{
									cursor = limit - v_3;
									goto lab3_brk;
								}
								// delete, line 108
								slice_del();
								break;
							
							case 2: 
								// (, line 109
								// or, line 109
								do 
								{
									v_4 = limit - cursor;
									do 
									{
										// (, line 109
										// call R2, line 109
										if (!r_R2())
										{
											goto lab5_brk;
										}
										// delete, line 109
										slice_del();
										goto lab4_brk;
									}
									while (false);

lab5_brk: ;
									
									cursor = limit - v_4;
									// (, line 109
									// call R1, line 109
									if (!r_R1())
									{
										cursor = limit - v_3;
										goto lab3_brk;
									}
									// <-, line 109
									slice_from("eux");
								}
								while (false);

lab4_brk: ;
								
								break;
							
							case 3: 
								// (, line 111
								// call R2, line 111
								if (!r_R2())
								{
									cursor = limit - v_3;
									goto lab3_brk;
								}
								// delete, line 111
								slice_del();
								break;
							
							case 4: 
								// (, line 113
								// call RV, line 113
								if (!r_RV())
								{
									cursor = limit - v_3;
									goto lab3_brk;
								}
								// <-, line 113
								slice_from("i");
								break;
							}
					}
					while (false);

lab3_brk: ;
					
					break;
				
				case 7: 
					// (, line 119
					// call R2, line 120
					if (!r_R2())
					{
						return false;
					}
					// delete, line 120
					slice_del();
					// try, line 121
					v_5 = limit - cursor;
					do 
					{
						// (, line 121
						// [, line 122
						ket = cursor;
						// substring, line 122
						among_var = find_among_b(a_2, 3);
						if (among_var == 0)
						{
							cursor = limit - v_5;
							goto lab6_brk;
						}
						// ], line 122
						bra = cursor;
						switch (among_var)
						{
							
							case 0: 
								cursor = limit - v_5;
								goto lab6_brk;
							
							case 1: 
								// (, line 123
								// or, line 123
								do 
								{
									v_6 = limit - cursor;
									do 
									{
										// (, line 123
										// call R2, line 123
										if (!r_R2())
										{
											goto lab8_brk;
										}
										// delete, line 123
										slice_del();
										goto lab7_brk;
									}
									while (false);

lab8_brk: ;
									
									cursor = limit - v_6;
									// <-, line 123
									slice_from("abl");
								}
								while (false);

lab7_brk: ;
								break;
							
							case 2: 
								// (, line 124
								// or, line 124
								do 
								{
									v_7 = limit - cursor;
									do 
									{
										// (, line 124
										// call R2, line 124
										if (!r_R2())
										{
											goto lab10_brk;
										}
										// delete, line 124
										slice_del();
										goto lab9_brk;
									}
									while (false);

lab10_brk: ;
									
									cursor = limit - v_7;
									// <-, line 124
									slice_from("iqU");
								}
								while (false);

lab9_brk: ;

								break;
							
							case 3: 
								// (, line 125
								// call R2, line 125
								if (!r_R2())
								{
									cursor = limit - v_5;
									goto lab6_brk;
								}
								// delete, line 125
								slice_del();
								break;
							}
					}
					while (false);

lab6_brk: ;
					
					break;
				
				case 8: 
					// (, line 131
					// call R2, line 132
					if (!r_R2())
					{
						return false;
					}
					// delete, line 132
					slice_del();
					// try, line 133
					v_8 = limit - cursor;
					do 
					{
						// (, line 133
						// [, line 133
						ket = cursor;
						// literal, line 133
						if (!(eq_s_b(2, "at")))
						{
							cursor = limit - v_8;
							goto lab11_brk;
						}
						// ], line 133
						bra = cursor;
						// call R2, line 133
						if (!r_R2())
						{
							cursor = limit - v_8;
							goto lab11_brk;
						}
						// delete, line 133
						slice_del();
						// [, line 133
						ket = cursor;
						// literal, line 133
						if (!(eq_s_b(2, "ic")))
						{
							cursor = limit - v_8;
							goto lab11_brk;
						}
						// ], line 133
						bra = cursor;
						// or, line 133
						do 
						{
							v_9 = limit - cursor;
							do 
							{
								// (, line 133
								// call R2, line 133
								if (!r_R2())
								{
									goto lab13_brk;
								}
								// delete, line 133
								slice_del();
								goto lab12_brk;
							}
							while (false);

lab13_brk: ;
							
							cursor = limit - v_9;
							// <-, line 133
							slice_from("iqU");
						}
						while (false);

lab12_brk: ;
						
					}
					while (false);

lab11_brk: ;
					
					break;
				
				case 9: 
					// (, line 135
					// <-, line 135
					slice_from("eau");
					break;
				
				case 10: 
					// (, line 136
					// call R1, line 136
					if (!r_R1())
					{
						return false;
					}
					// <-, line 136
					slice_from("al");
					break;
				
				case 11: 
					// (, line 138
					// or, line 138
					do 
					{
						v_10 = limit - cursor;
						do 
						{
							// (, line 138
							// call R2, line 138
							if (!r_R2())
							{
								goto lab15_brk;
							}
							// delete, line 138
							slice_del();
							goto lab14_brk;
						}
						while (false);

lab15_brk: ;
						
						cursor = limit - v_10;
						// (, line 138
						// call R1, line 138
						if (!r_R1())
						{
							return false;
						}
						// <-, line 138
						slice_from("eux");
					}
					while (false);

lab14_brk: ;
					
					break;
				
				case 12: 
					// (, line 141
					// call R1, line 141
					if (!r_R1())
					{
						return false;
					}
					if (!(out_grouping_b(g_v, 97, 251)))
					{
						return false;
					}
					// delete, line 141
					slice_del();
					break;
				
				case 13: 
					// (, line 146
					// call RV, line 146
					if (!r_RV())
					{
						return false;
					}
					// fail, line 146
					// (, line 146
					// <-, line 146
					slice_from("ant");
					return false;
				
				case 14: 
					// (, line 147
					// call RV, line 147
					if (!r_RV())
					{
						return false;
					}
					// fail, line 147
					// (, line 147
					// <-, line 147
					slice_from("ent");
					return false;
				
				case 15: 
					// (, line 149
					// test, line 149
					v_11 = limit - cursor;
					// (, line 149
					if (!(in_grouping_b(g_v, 97, 251)))
					{
						return false;
					}
					// call RV, line 149
					if (!r_RV())
					{
						return false;
					}
					cursor = limit - v_11;
					// fail, line 149
					// (, line 149
					// delete, line 149
					slice_del();
					return false;
				}
			return true;
		}
		
		private bool r_i_verb_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			// setlimit, line 154
			v_1 = limit - cursor;
			// tomark, line 154
			if (cursor < I_pV)
			{
				return false;
			}
			cursor = I_pV;
			v_2 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_1;
			// (, line 154
			// [, line 155
			ket = cursor;
			// substring, line 155
			among_var = find_among_b(a_4, 35);
			if (among_var == 0)
			{
				limit_backward = v_2;
				return false;
			}
			// ], line 155
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					limit_backward = v_2;
					return false;
				
				case 1: 
					// (, line 161
					if (!(out_grouping_b(g_v, 97, 251)))
					{
						limit_backward = v_2;
						return false;
					}
					// delete, line 161
					slice_del();
					break;
				}
			limit_backward = v_2;
			return true;
		}
		
		private bool r_verb_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			// setlimit, line 165
			v_1 = limit - cursor;
			// tomark, line 165
			if (cursor < I_pV)
			{
				return false;
			}
			cursor = I_pV;
			v_2 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_1;
			// (, line 165
			// [, line 166
			ket = cursor;
			// substring, line 166
			among_var = find_among_b(a_5, 38);
			if (among_var == 0)
			{
				limit_backward = v_2;
				return false;
			}
			// ], line 166
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					limit_backward = v_2;
					return false;
				
				case 1: 
					// (, line 168
					// call R2, line 168
					if (!r_R2())
					{
						limit_backward = v_2;
						return false;
					}
					// delete, line 168
					slice_del();
					break;
				
				case 2: 
					// (, line 176
					// delete, line 176
					slice_del();
					break;
				
				case 3: 
					// (, line 181
					// delete, line 181
					slice_del();
					// try, line 182
					v_3 = limit - cursor;
					do 
					{
						// (, line 182
						// [, line 182
						ket = cursor;
						// literal, line 182
						if (!(eq_s_b(1, "e")))
						{
							cursor = limit - v_3;
							goto lab16_brk;
						}
						// ], line 182
						bra = cursor;
						// delete, line 182
						slice_del();
					}
					while (false);

lab16_brk: ;
					
					break;
				}
			limit_backward = v_2;
			return true;
		}
		
		private bool r_residual_suffix()
		{
			int among_var;
			int v_1;
			int v_2;
			int v_3;
			int v_4;
			int v_5;
			// (, line 189
			// try, line 190
			v_1 = limit - cursor;
			do 
			{
				// (, line 190
				// [, line 190
				ket = cursor;
				// literal, line 190
				if (!(eq_s_b(1, "s")))
				{
					cursor = limit - v_1;
					goto lab0_brk;
				}
				// ], line 190
				bra = cursor;
				// test, line 190
				v_2 = limit - cursor;
				if (!(out_grouping_b(g_keep_with_s, 97, 232)))
				{
					cursor = limit - v_1;
					goto lab0_brk;
				}
				cursor = limit - v_2;
				// delete, line 190
				slice_del();
			}
			while (false);

lab0_brk: ;
			
			// setlimit, line 191
			v_3 = limit - cursor;
			// tomark, line 191
			if (cursor < I_pV)
			{
				return false;
			}
			cursor = I_pV;
			v_4 = limit_backward;
			limit_backward = cursor;
			cursor = limit - v_3;
			// (, line 191
			// [, line 192
			ket = cursor;
			// substring, line 192
			among_var = find_among_b(a_6, 7);
			if (among_var == 0)
			{
				limit_backward = v_4;
				return false;
			}
			// ], line 192
			bra = cursor;
			switch (among_var)
			{
				
				case 0: 
					limit_backward = v_4;
					return false;
				
				case 1: 
					// (, line 193
					// call R2, line 193
					if (!r_R2())
					{
						limit_backward = v_4;
						return false;
					}
					// or, line 193
					do 
					{
						v_5 = limit - cursor;
						do 
						{
							// literal, line 193
							if (!(eq_s_b(1, "s")))
							{
								goto lab2_brk;
							}
							goto lab1_brk;
						}
						while (false);

lab2_brk: ;
						
						cursor = limit - v_5;
						// literal, line 193
						if (!(eq_s_b(1, "t")))
						{
							limit_backward = v_4;
							return false;
						}
					}
					while (false);

lab1_brk: ;

					// delete, line 193
					slice_del();
					break;
				
				case 2: 
					// (, line 195
					// <-, line 195
					slice_from("i");
					break;
				
				case 3: 
					// (, line 196
					// delete, line 196
					slice_del();
					break;
				
				case 4: 
					// (, line 197
					// literal, line 197
					if (!(eq_s_b(2, "gu")))
					{
						limit_backward = v_4;
						return false;
					}
					// delete, line 197
					slice_del();
					break;
				}
			limit_backward = v_4;
			return true;
		}
		
		private bool r_un_double()
		{
			int v_1;
			// (, line 202
			// test, line 203
			v_1 = limit - cursor;
			// among, line 203
			if (find_among_b(a_7, 5) == 0)
			{
				return false;
			}
			cursor = limit - v_1;
			// [, line 203
			ket = cursor;
			// next, line 203
			if (cursor <= limit_backward)
			{
				return false;
			}
			cursor--;
			// ], line 203
			bra = cursor;
			// delete, line 203
			slice_del();
			return true;
		}
		
		private bool r_un_accent()
		{
			int v_3;
			// (, line 206
			// atleast, line 207
			{
				int v_1 = 1;
				// atleast, line 207
				while (true)
				{
					do 
					{
						if (!(out_grouping_b(g_v, 97, 251)))
						{
							goto lab16_brk;
						}
						v_1--;
						goto replab1;
					}
					while (false);

lab16_brk: ;
					
					goto replab1_brk;

replab1: ;
				}

replab1_brk: ;
				
				if (v_1 > 0)
				{
					return false;
				}
			}
			// [, line 208
			ket = cursor;
			// or, line 208
lab16: 
			do 
			{
				v_3 = limit - cursor;
				do 
				{
					// literal, line 208
					if (!(eq_s_b(1, "\u00E9")))
					{
						goto lab16_brk;
					}
					goto lab16_brk;
				}
				while (false);

lab16_brk: ;
				
				cursor = limit - v_3;
				// literal, line 208
				if (!(eq_s_b(1, "\u00E8")))
				{
					return false;
				}
			}
			while (false);
			// ], line 208
			bra = cursor;
			// <-, line 208
			slice_from("e");
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
			// (, line 212
			// do, line 214
			v_1 = cursor;
			do 
			{
				// call prelude, line 214
				if (!r_prelude())
				{
					goto lab0_brk;
				}
			}
			while (false);

lab0_brk: ;
			
			cursor = v_1;
			// do, line 215
			v_2 = cursor;
			do 
			{
				// call mark_regions, line 215
				if (!r_mark_regions())
				{
					goto lab1_brk;
				}
			}
			while (false);

lab1_brk: ;
			
			cursor = v_2;
			// backwards, line 216
			limit_backward = cursor; cursor = limit;
			// (, line 216
			// do, line 218
			v_3 = limit - cursor;
			do 
			{
				// (, line 218
				// or, line 228
				do 
				{
					v_4 = limit - cursor;
					do 
					{
						// (, line 219
						// and, line 224
						v_5 = limit - cursor;
						// (, line 220
						// or, line 220
						do 
						{
							v_6 = limit - cursor;
							do 
							{
								// call standard_suffix, line 220
								if (!r_standard_suffix())
								{
									goto lab6_brk;
								}
								goto lab5_brk;
							}
							while (false);

lab6_brk: ;
							
							cursor = limit - v_6;
							do 
							{
								// call i_verb_suffix, line 221
								if (!r_i_verb_suffix())
								{
									goto lab7_brk;
								}
								goto lab5_brk;
							}
							while (false);

lab7_brk: ;
							
							cursor = limit - v_6;
							// call verb_suffix, line 222
							if (!r_verb_suffix())
							{
								goto lab4_brk;
							}
						}
						while (false);

lab5_brk: ;
						
						cursor = limit - v_5;
						// try, line 225
						v_7 = limit - cursor;
						do 
						{
							// (, line 225
							// [, line 225
							ket = cursor;
							// or, line 225
							do 
							{
								v_8 = limit - cursor;
								do 
								{
									// (, line 225
									// literal, line 225
									if (!(eq_s_b(1, "Y")))
									{
										goto lab10_brk;
									}
									// ], line 225
									bra = cursor;
									// <-, line 225
									slice_from("i");
									goto lab9_brk;
								}
								while (false);

lab10_brk: ;
								
								cursor = limit - v_8;
								// (, line 226
								// literal, line 226
								if (!(eq_s_b(1, "\u00E7")))
								{
									cursor = limit - v_7;
									goto lab8_brk;
								}
								// ], line 226
								bra = cursor;
								// <-, line 226
								slice_from("c");
							}
							while (false);

lab9_brk: ;
							
						}
						while (false);

lab8_brk: ;

						goto lab3_brk;
					}
					while (false);

lab4_brk: ;
					
					cursor = limit - v_4;
					// call residual_suffix, line 229
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
			// do, line 234
			v_9 = limit - cursor;
			do 
			{
				// call un_double, line 234
				if (!r_un_double())
				{
					goto lab11_brk;
				}
			}
			while (false);

lab11_brk: ;
			
			cursor = limit - v_9;
			// do, line 235
			v_10 = limit - cursor;
			do 
			{
				// call un_accent, line 235
				if (!r_un_accent())
				{
					goto lab12_brk;
				}
			}
			while (false);

lab12_brk: ;
			
			cursor = limit - v_10;
			cursor = limit_backward; // do, line 237
			v_11 = cursor;
			do 
			{
				// call postlude, line 237
				if (!r_postlude())
				{
					goto lab13_brk;
				}
			}
			while (false);

lab13_brk: ;
			
			cursor = v_11;
			return true;
		}
	}
}
