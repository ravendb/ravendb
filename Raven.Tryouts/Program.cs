using System;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Indexes;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using (var x = new UsingCustomLuceneAnalyzer())
				{
					x.map_reduce_used_for_counting();
				}
			}
		}
		public class User
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}
	}
}