//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Smuggler
{
	public static class Program
	{
		static void Main(string[] args)
		{
			if (args.Length < 3 || args[0] != "in" && args[0] != "out")
			{
				Console.WriteLine(@"
Raven Smuggler - Import/Export utility
Usage:
	- Import the dump.raven file to a local instance:
		Raven.Smuggler in http://localhost:8080/ dump.raven
	- Export a local instance to dump.raven:
		Raven.Smuggler out http://localhost:8080/ dump.raven

	  Optional arguments (after required arguments): 
			--only-indexes : exports only index definitions
			--include-attachments : also export attachments
");

				Environment.Exit(-1);
			}

			try
			{
				var instanceUrl = args[1];
				if (instanceUrl.EndsWith("/") == false)
					instanceUrl += "/";
				var file = args[2];
				var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions { Url = instanceUrl });
				switch (args[0])
				{
					case "in":
						smugglerApi.ImportData(file, skipIndexes: args.Any(arg => string.Equals(arg, "/skipIndexes", StringComparison.InvariantCultureIgnoreCase)));
						break;
					case "out":
						bool exportIndexesOnly = args.Any(arg => arg.Equals("--only-indexes"));
						bool inlcudeAttachments = args.Any(arg => arg.Equals("--include-attachments"));
						smugglerApi.ExportData(new ExportSpec(file, exportIndexesOnly, inlcudeAttachments));
						break;
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				Environment.Exit(-1);
			}
		}
	}
}