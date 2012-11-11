using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			int x = int.MaxValue - 1;
			Interlocked.Increment(ref x);
			Interlocked.Increment(ref x);
			Console.WriteLine(x);
		}


		public class Article
		{
			public string Text { get; set; }
			public DateTime Date { get; set; }
		}
	}
}