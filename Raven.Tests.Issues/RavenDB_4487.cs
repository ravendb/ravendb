// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4487.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4487 : RavenTest
    {
        [Fact]
        public void CanFetchRoutes()
        {
            using (var store = NewRemoteDocumentStore())
            {

                using (var request = store.DatabaseCommands.CreateRequest("/debug/routes", "GET"))
                {
                    var response = request.ReadResponseJson();
                    var jObject = response as RavenJObject;
                    Assert.NotNull(jObject);
                    Assert.True(jObject.Count > 100);
                }
            }
        }
    }
}