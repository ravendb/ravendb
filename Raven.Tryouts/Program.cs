using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Bundles.PeriodicBackups;
using Raven.Tests.Bundles.Replication.Bugs;
using Raven.Tests.Bundles.Versioning;

namespace Raven.Tryouts
{
	public class Person
	{
		public string FirstName { get; set; }

		public string LastName { get; set; }
	}

	class Program
	{
		static void Main(string[] args)
		{
		    for (int i = 0; i < 100; i++)
		    {
		        using (var x = new ConflictsWithRemote())
		        {
		            x.InnefficientMultiThreadedInsert();
		        }
		        Console.WriteLine(i);
		    }
		}

		private static void Save(IDocumentStore store, int j)
		{
			for (var i = 1; i <= 1000; i++)
			{
				store.DatabaseCommands.Put(string.Format("people/{0}/{1}", j, i), null, RavenJObject.FromObject(new Person
				{
					FirstName = "FirstName" + i,
					LastName = "LastName" + i
				}), new RavenJObject());
			}
		}
	}
}