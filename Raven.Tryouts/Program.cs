using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Faceted;
using System.Linq;

internal class Program
{
	private static void Main(string[] args)
	{
		using (var store = new DocumentStore
		{
			Url = "http://localhost:8080",
			DefaultDatabase = "dump"
		}.Initialize())
		{
			for (int k = 0; k < 5; k++)
			{
				var rand = new Random(9321);
				var longs = new List<long>();
				for (int i = 0; i < 100; i++)
				{
					var sp = Stopwatch.StartNew();
					var x = store.DatabaseCommands.Get("tracks/" + rand.Next(1, 999999));

					var q = store.DatabaseCommands.Query("Tracks/ByCommon", new IndexQuery
					{
						Query = "Query:" + x.DataAsJson.Value<string>("NormalizedTitle").Split().Last()
					}, new string[0]);

					longs.Add(sp.ElapsedMilliseconds);
				}

				Console.Write(longs.Average() + ", ");
			}
		}
	}
}
