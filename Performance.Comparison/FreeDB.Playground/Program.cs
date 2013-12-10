using System;
using System.Collections.Generic;
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
			var x = new XmcdFileParser(@"C:\Users\Ayende\Downloads\freedb-complete-20130901.tar.bz2", disk => { });
			x.Start();
		}

	}
}
