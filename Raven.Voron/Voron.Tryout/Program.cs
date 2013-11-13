using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Xml;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Tryout
{
	internal class Program
	{
		private static void Main()
		{
			//string[] names =
			//{
			//	"Treasa Tiano","Arnette Arnone","Michelina Matthias","Reggie Royston",
			//	"Rebekah Remy","Meredith Marten","Fletcher Fulton","Gia Gibbens",
			//	"Leon Lansing","Malik Mcneal","Dale Denbow",
			//	"Barrett Bulfer","Hee Heins","Mitzie Mccourt","Angela Arena",
			//	"Jackelyn Johns","Terri Toy","Dinah Dinwiddie","Sook Swasey",
			//	"Wai Walko","Corrin Cales","Luciano Lenk","Verline Vandusen",
			//	"Joellen Joynes","Babette Ballas","Ebony Esh","Josphine Junkin","Herminia Horrigan",
			//	"Chelsie Chiles","Marlys Matheson","Ruthanne Reilly",
			//	"Teressa Tomasello","Shani Squire","Michaele Montagna",
			//	"Cuc Corter","Derek Devries","Carylon Cupples","Margaretta Mannings",
			//	"Barbar Brunk","Eboni Emond","Genie Grosse",
			//	"Kristin Krebsbach","Livia Lecroy","Jeraldine Jetton","Jeanmarie Jan",
			//	"Carmelo Coll","Shizue Sugg","Irena Imai","Tam Troxel","Berenice Burkart"
			//};
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.GetInMemory()))
			{
				using (Transaction tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var rand = new Random(123);
					for (int i = 0; i < 3000; i++)
					{
						tx.State.Root.Add(tx, "users/" + i.ToString("0000"), new MemoryStream(new byte[500]));
					}

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{

					DebugStuff.RenderAndShow(tx, tx.State.Root.State.RootPageNumber,3);
				}
				
			}

		//	using (var x = File.OpenRead(@"C:\Work\Voron\Voron.Tryout\bin\Debug\test2\0000000000000000000.journal"))
		//	{
		//		var memoryStream = new MemoryStream();

		//		using (var s = new DeflateStream(memoryStream, CompressionLevel.Optimal, true))
		//		{
		//			x.Position = 4096 * 4;
		//			var array = new byte[4096];
		//			x.Read(array, 0, 4096);

		//			var sp = Stopwatch.StartNew();
		//			s.WriteDirect(array, 0, 4096);

		//			s.Flush();

		//			Console.WriteLine(sp.ElapsedMilliseconds);
		//		}

		//		Console.WriteLine(memoryStream.Length);
		//	}
		}
	}
}