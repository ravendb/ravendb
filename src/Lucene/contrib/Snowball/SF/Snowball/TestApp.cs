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
using System;
namespace SF.Snowball
{
	
	public class TestApp
	{
		[STAThread]
		public static void  Main(System.String[] args)
		{
			
			if (args.Length < 2)
			{
				ExitWithUsage();
			}
			
			System.Type stemClass = System.Type.GetType("SF.Snowball.Ext." + args[0] + "Stemmer");
			SnowballProgram stemmer = (SnowballProgram) System.Activator.CreateInstance(stemClass);
			System.Reflection.MethodInfo stemMethod = stemClass.GetMethod("stem", (new System.Type[0] == null)?new System.Type[0]:(System.Type[]) new System.Type[0]);
			
			System.IO.StreamReader reader;
			reader = new System.IO.StreamReader(new System.IO.FileStream(args[1], System.IO.FileMode.Open, System.IO.FileAccess.Read), System.Text.Encoding.Default);
			reader = new System.IO.StreamReader(reader.BaseStream, reader.CurrentEncoding);
			
			System.Text.StringBuilder input = new System.Text.StringBuilder();
			
			System.IO.Stream outstream = System.Console.OpenStandardOutput();
			
			if (args.Length > 2 && args[2].Equals("-o"))
			{
				outstream = new System.IO.FileStream(args[3], System.IO.FileMode.Create);
			}
			else if (args.Length > 2)
			{
				ExitWithUsage();
			}
			
			System.IO.StreamWriter output = new System.IO.StreamWriter(outstream, System.Text.Encoding.Default);
			output = new System.IO.StreamWriter(output.BaseStream, output.Encoding);
			
			int repeat = 1;
			if (args.Length > 4)
			{
				repeat = System.Int32.Parse(args[4]);
			}
			
			System.Object[] emptyArgs = new System.Object[0];
			int character;
			while ((character = reader.Read()) != - 1)
			{
				char ch = (char) character;
				if (System.Char.IsWhiteSpace(ch))
				{
					if (input.Length > 0)
					{
						stemmer.SetCurrent(input.ToString());
						for (int i = repeat; i != 0; i--)
						{
							stemMethod.Invoke(stemmer, (System.Object[]) emptyArgs);
						}
						output.Write(stemmer.GetCurrent());
						output.Write('\n');
						input.Remove(0, input.Length - 0);
					}
				}
				else
				{
					input.Append(System.Char.ToLower(ch));
				}
			}
			output.Flush();
		}
		
		private static void  ExitWithUsage()
		{
			System.Console.Error.WriteLine("Usage: TestApp <stemmer name> <input file> [-o <output file>]");
			System.Environment.Exit(1);
		}
	}
}