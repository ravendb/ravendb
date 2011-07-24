//-----------------------------------------------------------------------
// <copyright file="LastModifiedLocal.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
    public class LastModifiedLocal : LocalClientTest
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using(var store = NewDocumentStore())
            {
                DateTime before;
                DateTime after;

                using(var session = store.OpenSession())
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
                    Assert.InRange(lastModified.ToUniversalTime(), before, after);
                    Assert.Equal(DateTimeKind.Utc, lastModified.Kind);
                }
            }
        }
    }
}