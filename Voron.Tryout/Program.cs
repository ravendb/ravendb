using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Xml;
using Voron.Debugging;
using Voron.Impl;
using Voron.Tests.Storage;

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

			using (var x = new BigValues())
			{
				x.CanReuseLargeSpace(4);
			}
		}
	}
}