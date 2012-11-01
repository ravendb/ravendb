using System;
using System.Collections.Generic;
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
			var dateTime = DateTime.Parse("11/01/2012 23:30:00Z");
			Console.WriteLine(TimeZoneInfo.ConvertTimeBySystemTimeZoneId(dateTime, "Pacific Standard Time"));
		}


		public class Article
		{
			public string Text { get; set; }
		}
	}
}