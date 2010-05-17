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

namespace Lucene.Net.Demo.Html
{
	
	class Test
	{
		[STAThread]
		public static void  Main(System.String[] argv)
		{
			if ("-dir".Equals(argv[0]))
			{
				System.String[] files = System.IO.Directory.GetFileSystemEntries(new System.IO.FileInfo(argv[1]).FullName);
				System.Array.Sort(files);
				for (int i = 0; i < files.Length; i++)
				{
					System.Console.Error.WriteLine(files[i]);
					System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(argv[1], files[i]));
					Parse(file);
				}
			}
			else
				Parse(new System.IO.FileInfo(argv[0]));
		}
		
		public static void  Parse(System.IO.FileInfo file)
		{
			System.IO.FileStream fis = null;
			try
			{
				fis = new System.IO.FileStream(file.FullName, System.IO.FileMode.Open, System.IO.FileAccess.Read);
				HTMLParser parser = new HTMLParser(fis);
				System.Console.Out.WriteLine("Title: " + Entities.Encode(parser.GetTitle()));
				System.Console.Out.WriteLine("Summary: " + Entities.Encode(parser.GetSummary()));
				System.Console.Out.WriteLine("Content:");
				System.IO.StreamReader reader = new System.IO.StreamReader(parser.GetReader().BaseStream, parser.GetReader().CurrentEncoding);
				for (System.String l = reader.ReadLine(); l != null; l = reader.ReadLine())
					System.Console.Out.WriteLine(l);
			}
			finally
			{
				if (fis != null)
					fis.Close();
			}
		}
	}
}