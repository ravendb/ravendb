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
    public class ExcludesWebServerHttpHeadersWhenDeserializing : NoDisposalNeeded
    {
        [Fact]
        public void CaseInsensitiveMatch()
        {
            var webHeaderCollection = new NameValueCollection
            {
                {Constants.LastModified, "2012-12-26T06:34:57.8189446Z"},
                {"WWW-Authenticate", "Negotiate oYG3MIG0oAMKAQChCwYJKoZIgvcSAQICooGfBIGcYIGZBgkqhkiG9xIBAgICAG+BiT..."},
                {"WWW-Authenticate".ToUpper(), "Negotiate oYG3MIG0oAMKAQChCwYJKoZIgvcSAQICooGfBIGcYIGZBgkqhkiG9xIBAgICAG+BiT..."},
                {"WWW-Authenticate".ToLower(), "Negotiate oYG3MIG0oAMKAQChCwYJKoZIgvcSAQICooGfBIGcYIGZBgkqhkiG9xIBAgICAG+BiT..."},
                {"ETag", Guid.NewGuid().ToString()},
            };
            JsonDocument document = SerializationHelper.DeserializeJsonDocument("products/ravendb", new RavenJObject(), webHeaderCollection, HttpStatusCode.OK);
            
            // The WWW-Authenticate header should have been excluded from the metadata no matter the case
            Assert.DoesNotContain("www-authenticate", document.Metadata.Keys, StringComparer.OrdinalIgnoreCase);
        }
    }
}
