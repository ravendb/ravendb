//-----------------------------------------------------------------------
// <copyright file="LastModifiedLocal.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Bugs.Metadata
{
    public class LastModifiedLocal : RavenTestBase
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using (var store = GetDocumentStore())
            {
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
                    var user = session.Load<User>("users/1");
                    var lastModified = Convert.ToDateTime(session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.LastModified]);

                    Assert.NotNull(lastModified);
                    Assert.InRange(lastModified.ToUniversalTime(), before, after);

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
