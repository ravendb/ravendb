using System;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				var sp = Stopwatch.StartNew();
				using(var op = new RemoteBulkInsertOperation(new BulkInsertOptions(), (ServerClient)store.DatabaseCommands,
					batchSize: 512))
				{
					op.Report += Console.WriteLine;
					for (int i = 0; i < 1000 * 1000; i++)
					{
						op.Write("items/"+(i+1), new RavenJObject
						{
							{"Raven-Entity-Name", "Users"}
						}, new RavenJObject
						{
							{"Name", "Users#"+i}
						} );
					}
				}
				Console.WriteLine(sp.Elapsed);
			}
		}
	}
}