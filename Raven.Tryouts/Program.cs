using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Linq;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Bundles.PeriodicBackups;
using Raven.Tests.Bundles.Replication.Bugs;
using Raven.Tests.Bundles.Versioning;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);

				using (var x = new HiLoServerKeysNotExported())
				{
					x.Export_And_Import_Incremental_Indexes();
				}
			}
		}
	}
}