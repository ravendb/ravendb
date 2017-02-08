// -----------------------------------------------------------------------
//  <copyright file="RDBQA_18.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Document;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBQA_18 : RavenNewTestBase
    {
        [Fact]
        public void ShouldNotThrowNullReferenceException()
        {
            using (var store = new DocumentStore())
            {
                store.Replication.WaitAsync().Wait(); // should not throw
            }
        }
    }
}
