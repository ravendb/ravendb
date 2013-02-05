using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Faceted;

internal class Program
{
	private static void Main(string[] args)
	{
		for (int i = 0; i < 100; i++)
		{
			Console.Clear();
			Console.WriteLine(i);
			using (var x = new FacetedIndexLimit())
			{
				x.CanPerformFacetedLimitSearch_HitsAsc();
			}
		}
	}
}
