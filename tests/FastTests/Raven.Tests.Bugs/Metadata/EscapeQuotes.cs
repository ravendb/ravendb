//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Bugs.Metadata
{
    public class EscapeQuotes : RavenTestBase
    {
        [Fact(Skip = "NotImplementedException")]
        public void CanProperlyEscapeQuotesInMetadata_Local_1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User();
                    session.Store(user);
                    session.Advanced.GetMetadataFor(user).Add("Foo", "\"Bar\"");
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal("\"Bar\"", metadata["Foo"]);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void CanProperlyEscapeQuotesInMetadata_Local_2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User();
                    session.Store(user);
                    session.Advanced.GetMetadataFor(user).Add("Foo", "\\\"Bar\\\"");
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal("\\\"Bar\\\"", metadata["Foo"]);
                }
            }
        }
    }
}
