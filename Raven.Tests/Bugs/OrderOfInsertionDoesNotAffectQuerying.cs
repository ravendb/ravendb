//-----------------------------------------------------------------------
// <copyright file="OrderOfInsertionDoesNotAffectQuerying.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Database.Data;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class OrderOfInsertionDoesNotAffectQuerying : RavenTest
	{
		[Fact]
		public void Works()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/1", null,
										   new RavenJObject
										   {
											   {"Name", "user1"},
											   {"Tags", new RavenJArray(new[]{"abc", "def"})}
										   },
										   new RavenJObject());

				store.DatabaseCommands.Put("users/2", null,
										   new RavenJObject
										   {
											   {"Name", "user2"},
										   },
										   new RavenJObject());

				var queryResult = store.DatabaseCommands.Query("dynamic", new IndexQuery
				{
					Query = "Tags,:abc"
				}, new string[0]);

				Assert.Equal(1, queryResult.Results.Count);
			}
		}

		[Fact]
		public void DoesNotWorks()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/1", null,
										   new RavenJObject
										   {
											   {"Name", "user1"},
										   },
										   new RavenJObject());

				store.DatabaseCommands.Put("users/2", null,
										   new RavenJObject
										   {
											   {"Name", "user2"},
											   {"Tags", new RavenJArray(new[]{"abc", "def"})}
										   },
										   new RavenJObject());

				var queryResult = store.DatabaseCommands.Query("dynamic", new IndexQuery
				{
					Query = "Tags,:abc"
				}, new string[0]);

				Assert.Equal(1, queryResult.Results.Count);
			}
		}
	}
}
