//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Bugs.Metadata
{
    public class LastModifiedRemote : RavenTestBase
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            var name = "CanAccessLastModifiedAsMetadata_1";
            var doc = MultiDatabase.CreateDatabaseDocument(name);

            DoNotReuseServer();
            using (var store = new DocumentStore { Urls = UseFiddler(Server.WebUrls), Database = name }.Initialize())
            {
                store.Admin.Server.Send(new CreateDatabaseOperation(doc));
                DateTime before;
                DateTime after;

                using (var session = store.OpenSession())
                {
                    session.Store(new User());

                    before = SystemTime.UtcNow;
                    session.SaveChanges();
                    after = SystemTime.UtcNow;
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var lastModified = Convert.ToDateTime(session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.LastModified]);

                    Assert.NotNull(lastModified);
                    int msPrecision = 1000;
                    Assert.InRange(lastModified.ToUniversalTime(), before.AddMilliseconds(-msPrecision), after.AddMilliseconds(msPrecision));

                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
