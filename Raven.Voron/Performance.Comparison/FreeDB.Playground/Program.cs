using System;
using System.CodeDom;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace FreeDB.Playground
{
	class Program
	{
		static void Main()
		{
			//var x = new XmcdFileParser(@"C:\Users\Ayende\Downloads\freedb-complete-20130901.tar.bz2", new JsonFileDisksDestination());
			//x.Start();

            var x = new GzipFileParser("freedb.json.gzip", new VoronEntriesDestination());
            x.Start();

            //var sp = Stopwatch.StartNew();
			var freedbQueries = new FreeDbQueries("FreeDb");
            //Console.WriteLine(sp.Elapsed);
            //sp.Restart();
            //foreach (var disk in freedbQueries.FindByAlbumTitle("Vitalogy"))
            //{
            //    Console.WriteLine(disk.Artist + " " + disk.Title);
            //}
            //Console.WriteLine(sp.Elapsed);
		}
	}
}
