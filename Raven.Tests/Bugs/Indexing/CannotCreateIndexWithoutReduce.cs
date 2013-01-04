// -----------------------------------------------------------------------
//  <copyright file="Test.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class QueryWithStaticIndexesAndCommonBaseClass : RavenTest
	{
		[Fact]
		public void CanCreateCorrectIndexForNestedObjectWithReferenceId()
		{
			using (var store = NewDocumentStore())
			{
				new Roots_ByUserId().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Root
									  {
										  User = new UserReference
													 {
														 Id = "Users/1"
													 }
									  });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var entities = session.Query<Root, Roots_ByUserId>()
						.Customize(x => x.WaitForNonStaleResults());
					Assert.Equal(1, entities.Count());
				}
			}
		}

		public class Roots_ByUserId : AbstractIndexCreationTask<Root>
		{
			public Roots_ByUserId()
			{
				Map = cases => from e in cases
							   select new
							   {
								   User_Id = e.User.Id
							   };
			}
		}

		public class Root : Identifiable
		{
			public UserReference User { get; set; }
		}

		public class UserReference : Identifiable
		{
		}

		public class Identifiable
		{
			public string Id { get; set; }
		}
	}
}
