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
			new Basic().PageSplitsAllAround();
			//var env = new StorageEnvironment(MemoryMappedFile.CreateNew("test", 1024 * 1024 * 16));
			//var ms = new MemoryStream(Encoding.UTF8.GetBytes("val"));
			//using (var tx = env.NewTransaction())
			//{
			//	for (int i = 0; i < 32 * 2; i++)
			//	{
			//		ms.Position = 0;
			//		env.Root.Add(tx, string.Format("{0,5}", i), ms);
			//	}

			//	tx.Commit();
			//}

			//DebugStuff.RenderAndShow(env.NewTransaction(), env.Root.Root, 25);
		}
	}
}
