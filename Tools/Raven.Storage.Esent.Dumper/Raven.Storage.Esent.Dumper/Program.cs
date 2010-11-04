using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Utilities;

namespace Raven.Storage.Esent.Dumper
{
	class Program
	{
		static void Main(string[] args)
		{
			new Dbutil().DumpToCsv(args);
		}
	}
}
