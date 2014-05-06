// -----------------------------------------------------------------------
//  <copyright file="ProjectionShouldNotLoadDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ProjectionShouldNotLoadDocument : RavenTest
	{
		[Fact]
		public void WhenProjecting()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.Documents.Put("FOO", null, new RavenJObject { { "Name", "Ayende" } }, new RavenJObject(), null);
				WaitForIndexing(store);
				var result = store.DatabaseCommands.Query("dynamic", new IndexQuery
				 {
					 FieldsToFetch = new[] { "Name" }
				 }, new string[0]);

				// if this is lower case, then we loaded this from the index, not from the db
				Assert.Equal("foo", result.Results[0].Value<string>(Constants.DocumentIdFieldName));
			}
		}
	}
}