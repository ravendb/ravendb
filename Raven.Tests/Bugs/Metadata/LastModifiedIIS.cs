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
    public class LastModifiedIIS : IISClientTest
    {
        [Fact]
        public void CanAccessLastModifiedAsMetadata()
        {
            using (var store = GetDocumentStore().Initialize())
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
                    int msPrecision = 1000;
                    Assert.InRange(lastModified.ToUniversalTime(), before.AddMilliseconds(-msPrecision), after.AddMilliseconds(msPrecision));
                    Assert.Equal(DateTimeKind.Local, lastModified.Kind);
                }
            }
        }
    }
}