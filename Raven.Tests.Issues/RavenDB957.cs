// -----------------------------------------------------------------------
//  <copyright file="RavenDB957.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB957 : RavenTest
	{
		public class Role
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		 [Fact]
		 public void LazyWithoutSelectNew()
		{
			using (var documentStore =NewDocumentStore())
			{
				documentStore.RegisterListener(new ChrisDNF.NoStaleQueriesAllowed());
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Role { Name = "Admin" });
					session.SaveChanges();
				}

				// ok
				using (var session = documentStore.OpenSession())
				{
					var x = session.Query<Role>()
					       .Select(r => new {r.Name})
					       .Lazily()
					       .Value;

					Assert.Equal("Admin", x.First().Name);
				}

				// fails
				using (var session = documentStore.OpenSession())
				{
					var x = session.Query<Role>()
					       .Select(r => r.Name)
					       .Lazily()
					       .Value;
					Assert.Equal("Admin", x.First());
				}
			}
			 
		 }
	}
}