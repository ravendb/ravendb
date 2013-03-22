using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Linq;
using Raven.Tests.Bundles.PeriodicBackups;
using Raven.Tests.Bundles.Replication.Bugs;
using Raven.Tests.Bundles.Versioning;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var uri = new Uri("ftp://localhost/");
			Console.WriteLine(uri.Port);
		}
	}
}