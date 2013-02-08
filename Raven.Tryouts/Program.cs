using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Faceted;
using System.Linq;
using Raven.Tests.MailingList;

internal class Program
{
	private static void Main(string[] args)
	{
		using (var ds = new DocumentStore
		{
			Url = "http://localhost:8080",
			DefaultDatabase = "sql"
		}.Initialize())
		{
			int pages = 0;
			while (true)
			{
				using (var session = ds.OpenSession(new OpenSessionOptions
				{
					ForceReadFromMaster = true
				}))
				{
					session.Advanced.MaxNumberOfRequestsPerSession = 10000;
					var results = session.Query<User>()
					       .Take(1024)
					       .Skip(pages*1024)
					       .ToList();
					if (results.Count == 0)
						break;
					pages++;
				}
			}
		}
	}

	public class User
	{
		public string Name, Email;
		public string[] Phones;
	}
}
