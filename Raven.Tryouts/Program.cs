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
using Raven.Tests.Issues;

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
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < 1000*1000; i++)
					{
						bulkInsert.Store(new User {Name = "Users #" + i});
					}
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