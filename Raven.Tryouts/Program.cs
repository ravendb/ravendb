using System;
using System.Collections.Concurrent;
using System.ComponentModel.Composition.Hosting;
using System.Net;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Util;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Notifications;
using Xunit;

public class Program
{
	public static void Main()
	{
		var store = new DocumentStore
		{
			Url = "http://ravendb-app.cloudapp.net",
			DefaultDatabase = "Property-Test",
			Credentials = new NetworkCredential(@"RavenDbUser", "arOk11", "ravendb-app")
		}.Initialize();

		store.DatabaseCommands.PutIndex("test", new IndexDefinition
		{
			Map = "from doc in docs select new { doc.YourName }"
		},true);

		store.Dispose();
	}
}
