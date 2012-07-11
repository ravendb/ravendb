// //-----------------------------------------------------------------------
// // <copyright file="JimBolla.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
extern alias database;
using System;

using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.UniqueConstraints;
using Xunit;
using Xunit.Extensions;

namespace Raven.Bundles.Tests.UniqueConstraints.Bugs
{
	public class JimBolla : UniqueConstraintsTest
	{
		public class App
		{
			public string Id { get; set; }

			[UniqueConstraint]
			public string Name { get; set; }

			[UniqueConstraint]
			public string Realm { get; set; }

			public string RealmConstraint
			{
				get { return Realm == null ? null : Realm + "x_x"; }
				set { }
			}
		}

		[Theory]
		[InlineData("Name", "http://localhost/")] // green
		[InlineData(null, "http://localhost/")] // exception on SaveChanges()
		[InlineData("Name", null)] // exception on SaveChanges()
		public void Test_Local(string name, string realm)
		{
			using (var raven = DocumentStore.OpenSession())
			{
				raven.Store(new App { Name = name, Realm = realm });
				raven.SaveChanges();
			}

			using (var raven = DocumentStore.OpenSession())
			{
				if(realm != null)
				{
					Assert.NotNull(raven.LoadByUniqueConstraint<App>(x => x.Realm, realm));
				}
			}

			using (var raven = DocumentStore.OpenSession())
			{
				if(name != null)
				{
					Assert.NotNull(raven.LoadByUniqueConstraint<App>(x => x.Name, name));
				}
			}
		}

		[Theory]
		[InlineData("Name", "http://localhost/")] // green
		[InlineData(null, "http://localhost/")] // exception on SaveChanges()
		[InlineData("Name", null)] // exception on SaveChanges()
		public void Test_Remote(string name, string realm)
		{
			using (var server = new database::Raven.Database.Server.HttpServer(new database::Raven.Database.Config.RavenConfiguration
			                                                      	{
			                                                      		Port = 8079
			                                                      	}, DocumentStore.DocumentDatabase))
			using(var ds = new DocumentStore
			               	{
			               		Url = "http://localhost:8079",
								Conventions =
									{
										FailoverBehavior = FailoverBehavior.FailImmediately
									}
			               	})
			{
				server.StartListening();

				ds.RegisterListener(new UniqueConstraintsStoreListener());
				ds.Initialize();

				using (var raven = ds.OpenSession())
				{
					raven.Store(new App { Name = name, Realm = realm });
					raven.SaveChanges();
				}

				using (var raven = ds.OpenSession())
				{
					if (realm != null)
					{
						Assert.NotNull(raven.LoadByUniqueConstraint<App>(x => x.Realm, realm));
					}
				}

				using (var raven = ds.OpenSession())
				{
					if (name != null)
					{
						Assert.NotNull(raven.LoadByUniqueConstraint<App>(x => x.Name, name));
					}
				}
			}
		}
	}
}