using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using NLog;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Indexing;

namespace etobi.EmbeddedTest
{
	internal class Program
	{
		static void Main(string[] args)
		{
			var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			var s = documentStore.OpenSession().Query<Item3>().Where(x=>x.Age > 100).ToString();
			Console.WriteLine(s);
		}
	}

	public class Item3
	{
		public long Age { get; set; }
	}
}