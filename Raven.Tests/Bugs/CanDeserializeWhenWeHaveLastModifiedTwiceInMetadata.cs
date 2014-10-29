// -----------------------------------------------------------------------
//  <copyright file="CanDeserializeWhenWeHaveLastModifiedTwiceInMetadata.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class CanDeserializeWhenWeHaveLastModifiedTwiceInMetadata : NoDisposalNeeded
	{
		[Fact]
		public void LegacyRavenLastModifiedMetadata()
		{
			var webHeaderCollection = new NameValueCollection
			{
				{Constants.RavenLastModified, "2012-12-26T06:34:57.8189446Z"},
				{Constants.RavenLastModified, "[\"2012-12-23T07:36:31.281+02:00\",\"2012-12-25T20:13:54.1534138Z\"]"},
				{Constants.LastModified, "2012-12-26T06:34:57.8189446Z"},
				{"ETag", Guid.NewGuid().ToString()},
			};
			JsonDocument document = SerializationHelper.DeserializeJsonDocument("products/ravendb", new RavenJObject(), webHeaderCollection, HttpStatusCode.OK);
			Assert.Equal("2012-12-26T06:34:57.8189446Z", document.LastModified.Value.ToString("o"));
		}
	}
}