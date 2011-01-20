using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Server;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			var db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "Data2"
			});
			while (true)
			{
				var sp = Stopwatch.StartNew();

				db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:Item",
					PageSize = 1024
				});

				Console.WriteLine(sp.ElapsedMilliseconds);
			}
		}
	}
}
