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
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "Test",
			})
			{
				store.Initialize();
				store.Conventions.ShouldCacheRequest = url => false;
				store.MaxNumberOfCachedRequests = 0;
				store.EnlistInDistributedTransactions = false;

				using (store.DisableAggressiveCaching())
				{
					int c = 0;
					while (true)
					{
						Console.WriteLine(++c);
						using (var session = store.OpenSession())
						{
							for (var i = 0; i < 1000; i++)
								session.Store(new Article { Text = "foobar" });
							session.SaveChanges();
						}
					}
				}
			}
		}


		public class Article
		{
			public string Text { get; set; }
		}
	}
}