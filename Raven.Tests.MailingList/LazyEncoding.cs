// -----------------------------------------------------------------------
//  <copyright file="LazyEncoding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LazyEncoding : RavenTest
	{
		public class User
		{
			public string Name { get; set; }
			public bool Admin { get; set; }
		}

		public class UserIndex : AbstractIndexCreationTask<User>
		{
			public UserIndex()
			{
				Map = users =>
					from u in users
					select new {u.Name, u.Admin};
			}
		}

		[Fact]
		public void ShouldNotMatterForFacets()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new UserIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Name = "Oren",
						Admin = true
					});

					session.SaveChanges();
				}
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
						.Where("+Name:Oren +Name:Eini")
						.ToFacets(new Facet[]
						{
							new Facet<User>
							{
								Name = user => user.Admin
							}
						});

					Assert.Empty(x.Results["Admin"].Values);
				}

				using (var session = store.OpenSession())
				{
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
						.Where("+Name:Oren +Name:Eini")
						.ToFacetsLazy(new Facet[]
						{
							new Facet<User>
							{
								Name = user => user.Admin
							}
						}).Value;

					Assert.Empty(x.Results["Admin"].Values);
				}
			}
		}
	}
}