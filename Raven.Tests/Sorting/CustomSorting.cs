// -----------------------------------------------------------------------
//  <copyright file="CustomSorting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Jint.Parser;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Sorting
{
	public class CustomSorting : RavenTest
	{
		public class User
		{
			public string Name;
		}

		public class User_Search : AbstractIndexCreationTask<User>
		{
			public User_Search()
			{
				Map = users =>
					from user in users
					select new { user.Name };

				Store(x => x.Name, FieldStorage.Yes);
			}
		}

		[Fact]
		public void Normal()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Maxim" });
					session.Store(new User { Name = "Oren" });
					session.SaveChanges();
				}

				new User_Search().Execute(store);
				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var users = session.Query<User, User_Search>()
						.Customize(x => x.CustomSortUsing(typeof (SortByNumberOfCharactersFromEnd).AssemblyQualifiedName))
						.AddTransformerParameter("len", 1)
						.ToList();
				}
			}
		}
	}
}