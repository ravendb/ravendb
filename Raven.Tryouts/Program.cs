using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Faceted;


public class ChessTest
{
	private static EmbeddableDocumentStore documentStore;

	public static bool Startup(string[] args)
	{
		IOExtensions.DeleteDirectory(@"C:\Work\test\data");
		documentStore = new EmbeddableDocumentStore();
		InitDatabase(documentStore);
		return true;
	}

	public static bool Run()
	{
		var random = new Random();
		var dataToQueryFor = new List<string>();

		for (var i = 0; i < 10; i++)
		{
			using (var session = documentStore.OpenSession())
			{
				session.Query<Foo>("index" + random.Next(0, 5))
					.Where(x => x.Data == "dont care")
					.FirstOrDefault();

				var foo = new Foo { Id = Guid.NewGuid().ToString(), Data = Guid.NewGuid().ToString() };
				dataToQueryFor.Add(foo.Data);
				session.Store(foo);
				session.SaveChanges();
			}
		}

		using (var session = documentStore.OpenSession())
		{
			session.Query<Foo>("index" + random.Next(0, 5))
				.Customize(x => x.WaitForNonStaleResults())
				.Where(x => x.Data == dataToQueryFor[random.Next(0, 10)])
				.FirstOrDefault();
		}
		return true;
	}

	public static bool Shutdown()
	{
		documentStore.Dispose();
		return true;
	}

	public class Foo
	{
		public string Id { get; set; }
		public string Data { get; set; }
	}
	private static void InitDatabase(EmbeddableDocumentStore documentStore)
	{
		documentStore.Configuration.DataDirectory = @"C:\Work\test\data";
		documentStore.Configuration.DefaultStorageTypeName = "esent";
		documentStore.Initialize();

		new RavenDocumentsByEntityName().Execute(documentStore);

		for (var i = 0; i < 5; i++)
		{
			documentStore.DatabaseCommands.PutIndex("index" + i,
				new IndexDefinitionBuilder<Foo>
				{
					Map = docs => from doc in docs select new { doc.Data }
				});
		}
	}
}

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			ChessTest.Startup(null);
			ChessTest.Run();
		}
		
	}
}
