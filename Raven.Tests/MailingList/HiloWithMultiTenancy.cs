// -----------------------------------------------------------------------
//  <copyright file="HiloWithMultiTenancy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Document;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.MailingList
{
	public class HiloWithMultiTenancy : RavenTest
	{
		public class Item {}

		[Fact]
		public void ShouldGenerateHiloOnTheProperDatabase()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");

				using(var testSession = store.OpenSession("test"))
				{
					testSession.Store(new Item());
					testSession.SaveChanges();
				}

				using (var defSession = store.OpenSession())
				{
					Assert.Null(defSession.Load<object>("Raven/Hilo/Items"));
				}

				using (var testSession = store.OpenSession("test"))
				{
					Assert.NotNull(testSession.Load<object>("Raven/Hilo/Items"));
				}
			}
		}
	}
}