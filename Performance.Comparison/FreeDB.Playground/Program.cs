using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Win32.SafeHandles;
using XmcdParser;

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
		}

	}

	public class NullDisksDestination : DisksDestination
	{
		public override void Accept(Disk d)
		{
			
		}

		public override void Done()
		{
		}
	}
}
