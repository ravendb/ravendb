//-----------------------------------------------------------------------
// <copyright file="CanHandleDocumentRemoval.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class CanHandleDocumentRemoval : RavenTest
	{
		[Fact]
		public void CanHandleDocumentDeletion()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					for (int i = 0; i < 3; i++)
					{
						session.Store(new User
						{
							Name = "ayende"
						});
					}
					session.SaveChanges();
				}
		 
				using (var session = store.OpenSession())
				{
					var users = session.Query<User>("Raven/DocumentsByEntityName")
						.Customize(x => x.WaitForNonStaleResults())
						.ToArray();
					Assert.NotEmpty(users);
					foreach (var user in users)
					{
						session.Delete(user);
					}
					session.SaveChanges();
				}
		   
				using (var session = store.OpenSession())
				{
					var users = session.Query<User>("Raven/DocumentsByEntityName")
						.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
						.ToArray();
					Assert.Empty(users);
				}
			}
		}
	}
}
