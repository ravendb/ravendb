using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Nevar.Debugging;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
	unsafe class Program
	{
		static void Main(string[] args)
		{
			new Basic().PageSplitsAllAround();
			return;
			var env = new StorageEnvironment(MemoryMappedFile.CreateNew("test", 1024 * 1024 * 16));
			var ms = new MemoryStream(Encoding.UTF8.GetBytes("000000000000"));
			using (var tx = env.NewTransaction())
			{
				var cursor = tx.GetCursor(env.Root);
				for (int i = 0; i < 6; i++)
				{
					ms.Position = 0;
					env.Root.Add(tx, string.Format("{0,5}", i), ms);
				}

				DebugStuff.RenderAndShow(env.NewTransaction(), cursor.Root, 1);
				for (int i = 4; i >= 1 ; i--)
				{
					env.Root.Delete(tx, string.Format("{0,5}", i));
					DebugStuff.RenderAndShow(env.NewTransaction(), cursor.Root, 1);
				}

				tx.Commit();
			}
		}
	}
}
