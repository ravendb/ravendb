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
			for (int i = 0; i < 100; i++)
			{
				Console.WriteLine(i);
				using (var x = new PeriodicBackupTests())
					x.CanBackupToDirectory_MultipleBackups();
			}
		} 
	}
}