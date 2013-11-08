using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Tryout
{
	internal class Program
	{
		private static void Main()
		{
			string[] names =
			{
				"Treasa Tiano","Arnette Arnone","Michelina Matthias","Reggie Royston",
				"Rebekah Remy","Meredith Marten","Fletcher Fulton","Gia Gibbens",
				"Leon Lansing","Malik Mcneal","Dale Denbow",
				"Barrett Bulfer","Hee Heins","Mitzie Mccourt","Angela Arena",
				"Jackelyn Johns","Terri Toy","Dinah Dinwiddie","Sook Swasey",
				"Wai Walko","Corrin Cales","Luciano Lenk","Verline Vandusen",
				"Joellen Joynes","Babette Ballas","Ebony Esh","Josphine Junkin","Herminia Horrigan",
				"Chelsie Chiles","Marlys Matheson","Ruthanne Reilly",
				"Teressa Tomasello","Shani Squire","Michaele Montagna",
				"Cuc Corter","Derek Devries","Carylon Cupples","Margaretta Mannings",
				"Barbar Brunk","Eboni Emond","Genie Grosse",
				"Kristin Krebsbach","Livia Lecroy","Jeraldine Jetton","Jeanmarie Jan",
				"Carmelo Coll","Shizue Sugg","Irena Imai","Tam Troxel","Berenice Burkart"
			};

			Console.WriteLine(names.Length);
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath("asos")))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "users");
					env.CreateTree(tx, "users_names");

					tx.Commit();
				}

				//int x = 0;
				//var sp = Stopwatch.StartNew();
				//for (int j = 0; j < 1000; j++)
				//{
				//	using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				//	{
				//		var users = tx.GetTree("users");
				//		var usersNames = tx.GetTree("users_names");
				//		for (int i = 0; i < 1000; i++)
				//		{
				//			var name = names[i % names.Length];
				//			var key = "users/" + (x++);
				//			users.Add(tx, key, new MemoryStream(Encoding.UTF8.GetBytes(name)));
				//			usersNames.MultiAdd(tx, name.ToLower(), key);
				//			foreach (var part in name.Split())
				//			{
				//				usersNames.MultiAdd(tx, part.ToLower(), new Slice(Encoding.UTF8.GetBytes(key)));
				//			}
				//		}
				//		tx.Commit();
				//	}
				//}
				//Console.WriteLine("Write: {0}", sp.Elapsed);


				for (int xx = 1; xx < 32; xx++)
				{
					int threads = xx;
					var sp = Stopwatch.StartNew();
					int[] counts = new int[threads];

					Parallel.For(0, threads, i =>
					{
						int localCount = 0;
						var rand = new Random(i);
						var buffer = new byte[1024*16];
						using (var tx = env.NewTransaction(TransactionFlags.Read))
						{
							var users = tx.GetTree("users");
							using (var it = users.Iterate(tx))
							{
								if (it.Seek(Slice.BeforeAllKeys) == false)
									return;
								do
								{
									localCount++;
									
									using (var stream = it.CreateStreamForCurrent())
									{
										while (stream.Read(buffer,0,buffer.Length) != 0)
										{
									
										}
									}
								} while (it.MoveNext());
							}
						}
						counts[i] = localCount;
					});

					Console.WriteLine("{0} {1} {2:#,#} = {3:#,#}", xx, sp.Elapsed, counts.Sum(), Math.Round((double)counts.Sum() / Math.Max(1, sp.ElapsedMilliseconds / 1000), 2));
				}


				//while (true)
				//{
				//	using (var tx = env.NewTransaction(TransactionFlags.Read))
				//	{
				//		var users = tx.GetTree("users");
				//		var userNames = tx.GetTree("users_names");

				//		Console.Write("Give me a name: ");
				//		var name = Console.ReadLine();

				//		using (var it = userNames.MultiRead(tx, name.ToLower()))
				//		{
				//			if (it.Seek(Slice.BeforeAllKeys) == false)
				//			{
				//				Console.WriteLine("No such name!");
				//				continue;
				//			}

				//			for (int i = 0; i < 10; i++)
				//			{
				//				Console.WriteLine(it.CurrentKey.ToString());
				//				if (it.MoveNext() == false)
				//					break;
				//			}
				//		}

				//	}
				//}
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
			//			s.Write(array, 0, 4096);

			//			s.Flush();

			//			Console.WriteLine(sp.ElapsedMilliseconds);
			//		}

			//		Console.WriteLine(memoryStream.Length);
			//	}
		}
	}
}