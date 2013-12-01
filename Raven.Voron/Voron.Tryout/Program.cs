using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Xml;
using Voron.Debugging;
using Voron.Impl;
using Voron.Tests.Bugs;
using Voron.Tests.Storage;

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

			using (var db = new StorageEnvironment(StorageEnvironmentOptions.ForPath("nyc")))
			{
				//using (var tx = db.NewTransaction(TransactionFlags.ReadWrite))
				//{
				//	int index = 0;
				//	foreach (var name in names)
				//	{
				//		tx.State.Root.Add(tx, "users/" + (index++), new MemoryStream(Encoding.UTF8.GetBytes(name)));
				//	}

				//	tx.Commit();
				//}

				using (var tx = db.NewTransaction(TransactionFlags.Read))
				{
					using (var it = tx.State.Root.Iterate(tx))
					{
						if (it.Seek(Slice.BeforeAllKeys) == false)
							return;
						do
						{
							var key = it.CurrentKey.ToString();
							using (var stream = it.CreateStreamForCurrent())
							{
								var value = new StreamReader(stream).ReadToEnd();
								Console.WriteLine(key + ": " + value);
							}
						} while (it.MoveNext());
					}
				}
			}
		
		}
	}
}