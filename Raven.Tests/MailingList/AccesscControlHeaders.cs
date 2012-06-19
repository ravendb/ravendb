// -----------------------------------------------------------------------
//  <copyright file="AccesscControlHeaders.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class AccesscControlHeaders : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.AccessControlAllowOrigin = "*";
		}

		[Fact]
		public void CanWork()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}

				for (int i = 0; i < 3; i++)
				{
					using (var session = store.OpenSession())
					{
						session.Load<Item>(1).Count++;
						session.SaveChanges();
					}
				}
				using (var session = store.OpenSession())
				{
					var load = session.Load<Item>(1);
					var ravenJToken = session.Advanced.GetMetadataFor(load)["Access-Control-Allow-Origin"];
					Assert.True(ravenJToken == null || ravenJToken.Type != JTokenType.Array);
				}

			}
		}
		public class Item
		{
			public int Count;
		}
	}
}