using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Bundles.Expiration;
using Raven.Tests.Faceted;
using System.Linq;

internal class Program
{
	private static void Main(string[] args)
	{
		for (int i = 0; i < 100; i++)
		{
			Console.Clear();
			Console.WriteLine(i);
			using (var x = new Expiration())
			{
				x.Can_add_entity_with_expiry_then_read_it_before_it_expires();
			}
		}
	}
}
