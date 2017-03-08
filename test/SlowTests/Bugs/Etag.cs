//-----------------------------------------------------------------------
// <copyright file="Etag.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class Etag : RavenTestBase
    {
        [Fact]
        public void WhenSaving_ThenGetsEtag()
        {
            using (var store = GetDocumentStore())
            {
                var foo = new IndexWithTwoProperties.Foo {Id = Guid.NewGuid().ToString(), Value = "foo"};

                using (var session = store.OpenSession())
                {
                    session.Store(foo);

                    session.SaveChanges();
                    
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    Assert.NotNull(metadata["@etag"]);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<IndexWithTwoProperties.Foo>(foo.Id);

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    Assert.NotNull(metadata["@etag"]);

                }
            }
        }

    }
}
