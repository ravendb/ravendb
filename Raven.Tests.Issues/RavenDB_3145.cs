// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3145.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3145 : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				var result1 = store.DatabaseCommands.Put("key/1", null, new RavenJObject(), new RavenJObject());
				var result2 = store.DatabaseCommands.Put("key/1", null, new RavenJObject(), new RavenJObject());

				var e = Assert.Throws<ConcurrencyException>(() => store.DatabaseCommands.Delete("key/1", result1.ETag));
				Assert.Equal("DELETE attempted on document 'key/1' using a non current etag", e.Message);
			}
		}
	}
}