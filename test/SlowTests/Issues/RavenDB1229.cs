// -----------------------------------------------------------------------
//  <copyright file="RavenDB1229.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB1229 : RavenTestBase
    {
        public RavenDB1229(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DeleteByNotExistingIndex()
        {
            using (var store = GetDocumentStore())
            {
                try
                {
                    var op = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery
                    {
                        Query = "FROM INDEX 'noSuchIndex' WHERE Tag = 'Animals'"
                    }));

                    op.WaitForCompletion(TimeSpan.FromSeconds(15));

                    Assert.False(true, "Should have thrown");
                }
                catch (Exception e)
                {
                    Assert.NotNull(e);
                }
            }
        }
    }
}
