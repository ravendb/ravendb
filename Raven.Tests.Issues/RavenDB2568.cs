// -----------------------------------------------------------------------
//  <copyright file="RavenDB2568.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB2568 : RavenTest
	{
		[Fact]
		public void simpleSkipAfter()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/01", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/02", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/03", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/10", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/12", null, new RavenJObject(), new RavenJObject());

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.LoadStartingWith<object>("users/",skipAfter:"users/02");
					Assert.Equal(3, results.Length);
					Assert.Equal("users/03", session.Advanced.GetDocumentId(results[0]));
					Assert.Equal("users/10", session.Advanced.GetDocumentId(results[1]));
					Assert.Equal("users/12", session.Advanced.GetDocumentId(results[2]));
				}
			}
		}

		[Fact]
		public void StreamingSkipAfter()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/01", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/02", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/03", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/10", null, new RavenJObject(), new RavenJObject());
				store.DatabaseCommands.Put("users/12", null, new RavenJObject(), new RavenJObject());

				using (var session = store.OpenSession())
				{
					var results = session.Advanced.Stream<dynamic>(startsWith: "users/", skipAfter: "users/02");
					
					Assert.True(results.MoveNext());
					Assert.Equal("users/03", results.Current.Key);
					Assert.True(results.MoveNext());
					Assert.Equal("users/10", results.Current.Key);
					Assert.True(results.MoveNext());
					Assert.Equal("users/12", results.Current.Key);
					Assert.False(results.MoveNext());
					
				}
			}
		}
	}
}