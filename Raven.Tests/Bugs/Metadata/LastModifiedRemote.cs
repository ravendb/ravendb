//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
    public class LastModifiedRemote : RemoteClientTest
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using(GetNewServer())
            using (var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
            {
                DateTime before;
                DateTime after;

                using (var session = store.OpenSession())
                {
                    session.Store(new User());

                    before = DateTime.UtcNow;
                    session.SaveChanges();
                    after = DateTime.UtcNow;
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var lastModified = session.Advanced.GetMetadataFor(user).Value<DateTime>("Last-Modified");
                    Assert.NotNull(lastModified);
                    Assert.InRange(lastModified.ToUniversalTime().Ticks, before.Ticks, after.Ticks);
                    Assert.Equal(DateTimeKind.Utc, lastModified.Kind);
                }
            }
        }
    }
}