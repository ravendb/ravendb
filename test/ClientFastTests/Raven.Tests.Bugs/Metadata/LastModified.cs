//-----------------------------------------------------------------------
// <copyright file="LastModifiedLocal.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Abstractions.Data;
using Raven.NewClient.Abstractions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Bugs.Metadata
{
    public class LastModified : RavenNewTestBase
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

                    before = DateTime.UtcNow;
                    session.SaveChanges();
                    after = DateTime.UtcNow;
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var lastModified = DateTime.Parse(session.Advanced.GetMetadataFor(user)[Constants.Metadata.LastModified]);
                    Assert.NotNull(lastModified);
                    Assert.InRange(lastModified, before, after);
                    //TODO
                    //Assert.Equal(DateTimeKind.Utc, lastModified.Kind);
                }

                //WaitForUserToContinueTheTest(store);
            }
        }
    }
}
