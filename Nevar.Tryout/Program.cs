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
			//new Basic().PageSplitsAllAround();
			var env = new StorageEnvironment(MemoryMappedFile.CreateNew("test", 1024 * 1024 * 16));
			var ms = new MemoryStream(Encoding.UTF8.GetBytes("val"));
			using (var tx = env.NewTransaction())
			{
				var cursor = tx.GetCursor(env.Root);
				for (int i = 0; i < 1024 / 2; i++)
				{
					var root = cursor.Root;
					ms.Position = 0;
					if (i == 227)
					{
						DebugStuff.RenderAndShow(tx, root);

					}
					env.Root.Add(tx, string.Format("{0,5}", i), ms);
				}

				tx.Commit();
			}

			DebugStuff.RenderAndShow(env.NewTransaction(), env.Root.Root, 50, "svg");
		}
	}
}
