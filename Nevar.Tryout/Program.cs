using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
	unsafe class Program
	{
		static void Main(string[] args)
		{
			//Slice a = "test-1";
			//Slice b = "test-10";

			//var compare = a.Compare(b, NativeMethods.memcmp);
			//Console.WriteLine(compare);
			new Basic().CanAddEnoughToCausePageSplit();
			//var env = new StorageEnvironment(MemoryMappedFile.CreateNew("test", 1024*1024));
			//var ms = new MemoryStream(Encoding.UTF8.GetBytes("val"));
			//using (var tx = env.NewTransaction())
			//{
			//	for (int i = 0; i < 50; i++)
			//	{
			//		ms.Position = 0;
			//		env.Root.Add(tx, "test-" + i, ms);
			//	}

			//	tx.Commit();
			//}

			//env.Root.Dump(env.NewTransaction(),"out.dot");

			////Process.Start("start", "out.png");


		}
	}
}
