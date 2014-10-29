// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2344.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2344 : RavenTest
	{
		[Fact]
		public void EtagShouldBeStrippedOutFromTheMetadataOnPut()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Batch(new List<ICommandData>
				                             {
					                             new PutCommandData
					                             {
						                             Key = "key/1",
													 Etag = null,
													 Document = new RavenJObject(),
													 Metadata = new RavenJObject
													            {
														            { "etag", Etag.Empty.ToString() }
													            }
					                             }
				                             });

				var result = store.DatabaseCommands.Get(new[] { "key/1" }, null);

				Assert.Equal(1, result.Results.Count);

				var metadata = result.Results[0].Value<RavenJObject>(Constants.Metadata);

				Assert.NotNull(metadata);
				Assert.True(metadata.Keys.Contains("@etag"));
				Assert.False(metadata.Keys.Contains("etag"));
			}
		}
	}
}