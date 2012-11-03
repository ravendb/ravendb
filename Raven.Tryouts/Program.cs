using System;
using System.Collections.Generic;
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
			var date = new DateTime(2012, 11, 1, 0, 0, 0, DateTimeKind.Unspecified);
			var x= new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();
			var documentSession = x.OpenSession();
			var documentQuery = documentSession.Advanced.LuceneQuery<Article>();
			documentQuery.WhereBetweenOrEqual(xa=> xa.Date, date, date.AddDays(1));
			Console.WriteLine(documentQuery.ToString());
		}


		public class Article
		{
			public string Text { get; set; }
			public DateTime Date { get; set; }
		}
	}
}