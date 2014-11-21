// -----------------------------------------------------------------------
//  <copyright file="LastCollectionEtagsShouldHandleTouchedDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class LastCollectionEtagsShouldHandleTouchedDocuments : RavenTest
	{
		public class People_ByStreet : AbstractIndexCreationTask<Person>
		{
			public People_ByStreet()
			{
				Map = persons => from p in persons
								 select new
								 {
									 LoadDocument<Address>(p.AddressId).Street
								 };
			}
		}

		[Fact]
		public void ShouldProperlyDetermineStaleness()
		{
			using (var store = NewDocumentStore())
			{
				new People_ByStreet().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Address()
					{
						Id = "addresses/1",
						Street = "street 1"
					});

					session.Store(new Person()
					{
						AddressId = "addresses/1"
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);

				store.DatabaseCommands.Admin.StopIndexing();

				var queryResult = store.DatabaseCommands.Query("People/ByStreet", new IndexQuery());

				Assert.False(queryResult.IsStale);

				using (var session = store.OpenSession())
				{
					session.Store(new Address()
					{
						Id = "addresses/1",
						Street = "street 2"
					});

					session.SaveChanges();
				}

				queryResult = store.DatabaseCommands.Query("People/ByStreet", new IndexQuery());

				Assert.True(queryResult.IsStale);
			}
		}
	}
}