// -----------------------------------------------------------------------
//  <copyright file="RDoc_76.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Util;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RDoc_76 : RavenTest
	{
		private class Room
		{
			public string Sth { get; set; }
		}

		private class Kitchen : Room
		{
		}

		private class Bedroom : Room
		{
		}

		private class Guestroom : Bedroom
		{
		}

		private class MasterBedroom : Bedroom
		{
		}

		[Fact]
		public void RegisterIdConventionShouldWorkProperlyForDerivedTypes()
		{
			using (var store = NewDocumentStore())
			{
				store.Conventions.RegisterIdConvention<Bedroom>((dbName, cmds, r) => "b/" + r.Sth);
				store.Conventions.RegisterIdConvention<Guestroom>((dbName, cmds, r) => "gr/" + r.Sth);
				store.Conventions.RegisterIdConvention<Room>((dbName, cmds, r) => "r/" + r.Sth);
				store.Conventions.RegisterIdConvention<Kitchen>((dbName, cmds, r) => "k/" + r.Sth);
				store.Conventions.RegisterIdConvention<MasterBedroom>((dbName, cmds, r) => "mb/" + r.Sth);

				using (var session = store.OpenSession())
				{
					session.Store(new MasterBedroom { Sth = "1" });
					session.Store(new Guestroom { Sth = "2" });
					session.Store(new Bedroom { Sth = "3" });
					session.Store(new Room { Sth = "4" });
					session.Store(new Kitchen { Sth = "5" });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var mbs = session
						.Query<MasterBedroom>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
				}

				var mb = store.DatabaseCommands.Get("mb/1");
				var gr = store.DatabaseCommands.Get("gr/2");
				var b = store.DatabaseCommands.Get("b/3");
				var rm = store.DatabaseCommands.Get("r/4");
				var k = store.DatabaseCommands.Get("k/5");

				Assert.NotNull(mb);
				Assert.NotNull(gr);
				Assert.NotNull(b);
				Assert.NotNull(rm);
				Assert.NotNull(k);
			}
		}

		[Fact]
		public void RegisterIdConventionShouldWorkProperlyForDerivedTypesAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.RegisterAsyncIdConvention<Bedroom>((dbName, cmds, r) => new CompletedTask<string>("b/" + r.Sth));
				store.Conventions.RegisterAsyncIdConvention<Guestroom>((dbName, cmds, r) => new CompletedTask<string>("gr/" + r.Sth));
				store.Conventions.RegisterAsyncIdConvention<Room>((dbName, cmds, r) => new CompletedTask<string>("r/" + r.Sth));
				store.Conventions.RegisterAsyncIdConvention<Kitchen>((dbName, cmds, r) => new CompletedTask<string>("k/" + r.Sth));
				store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, cmds, r) => new CompletedTask<string>("mb/" + r.Sth));

				using (var session = store.OpenAsyncSession())
				{
					session.Store(new MasterBedroom { Sth = "1" });
					session.Store(new Guestroom { Sth = "2" });
					session.Store(new Bedroom { Sth = "3" });
					session.Store(new Room { Sth = "4" });
					session.Store(new Kitchen { Sth = "5" });

					session.SaveChangesAsync().Wait();
				}

				using (var session = store.OpenSession())
				{
					var mbs = session
						.Query<MasterBedroom>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
				}

				var mb = store.DatabaseCommands.Get("mb/1");
				var gr = store.DatabaseCommands.Get("gr/2");
				var b = store.DatabaseCommands.Get("b/3");
				var rm = store.DatabaseCommands.Get("r/4");
				var k = store.DatabaseCommands.Get("k/5");

				Assert.NotNull(mb);
				Assert.NotNull(gr);
				Assert.NotNull(b);
				Assert.NotNull(rm);
				Assert.NotNull(k);
			}
		}

		[Fact]
		public void ThrowInvalidOperationExceptionIfConventionExistsForOtherTypeOfOperationButDoesntForCurrentType()
		{
			var exception = Assert.Throws<InvalidOperationException>(() =>
			{
				using (var store = NewRemoteDocumentStore())
				{
					store.Conventions.RegisterAsyncIdConvention<Bedroom>((dbName, cmds, r) => new CompletedTask<string>("b/" + r.Sth));

					using (var session = store.OpenSession())
					{
						session.Store(new Bedroom { Sth = "3" });

						session.SaveChanges();
					}
				}
			});

			Assert.Equal("Id covention for synchronous operation was not found for entity Raven.Tests.Issues.RDoc_76+Bedroom, but convention for asynchronous operation exists.", exception.Message);

			exception = Assert.Throws<InvalidOperationException>(() =>
			{
				using (var store = NewRemoteDocumentStore())
				{
					store.Conventions.RegisterIdConvention<Bedroom>((dbName, cmds, r) => "b/" + r.Sth);

					using (var session = store.OpenAsyncSession())
					{
						session.Store(new Bedroom {Sth = "3"});

						session.SaveChangesAsync().Wait();
					}
				}
			});

			Assert.Equal("Id covention for asynchronous operation was not found for entity Raven.Tests.Issues.RDoc_76+Bedroom, but convention for synchronous operation exists.", exception.Message);
		}

		[Fact]
		public void RegisteringConventionForSameTypeShouldOverrideOldOne()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.RegisterIdConvention<MasterBedroom>((dbName, cmds, r) => "a/" + r.Sth);
				store.Conventions.RegisterIdConvention<MasterBedroom>((dbName, cmds, r) => "mb/" + r.Sth);

				using (var session = store.OpenSession())
				{
					session.Store(new MasterBedroom { Sth = "1" });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var mbs = session
						.Query<MasterBedroom>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
				}

				var a = store.DatabaseCommands.Get("a/1");
				var mb = store.DatabaseCommands.Get("mb/1");

				Assert.Null(a);
				Assert.NotNull(mb);
			}
		}

		[Fact]
		public void RegisteringConventionForSameTypeShouldOverrideOldOneAsync()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, cmds, r) => new CompletedTask<string>("a/" + r.Sth));
				store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, cmds, r) => new CompletedTask<string>("mb/" + r.Sth));

				using (var session = store.OpenAsyncSession())
				{
					session.Store(new MasterBedroom { Sth = "1" });

					session.SaveChangesAsync().Wait();
				}

				using (var session = store.OpenSession())
				{
					var mbs = session
						.Query<MasterBedroom>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
				}

				var a = store.DatabaseCommands.Get("a/1");
				var mb = store.DatabaseCommands.Get("mb/1");

				Assert.Null(a);
				Assert.NotNull(mb);
			}
		}
	}
}