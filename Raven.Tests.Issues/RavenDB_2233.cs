// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2233.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2233 : RavenTest
	{
		[Fact]
		public void CanMultipleQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Name = "user1"
					}, "users/1");
					session.Store(new User
					{
						Name = "user1"
					}, "users/2");
					session.Store(new User
					{
						Name = "user1"
					}, "users/3");
					session.SaveChanges();
				}

				var result = store.DatabaseCommands.Get(new[] { "users/3", "users/2", "users/1", "users/999", "users/2" }, new string[] { });
				Assert.Equal(5, result.Results.Count);
				Assert.Equal("users/3", result.Results[0]["@metadata"].Value<string>("@id"));
				Assert.Equal("users/2", result.Results[1]["@metadata"].Value<string>("@id"));
				Assert.Equal("users/1", result.Results[2]["@metadata"].Value<string>("@id"));
				Assert.Equal(null, result.Results[3]);
				Assert.Equal("users/2", result.Results[4]["@metadata"].Value<string>("@id"));
			}
		}
	}
}