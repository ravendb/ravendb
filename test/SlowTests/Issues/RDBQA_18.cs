// -----------------------------------------------------------------------
//  <copyright file="RDBQA_18.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Client.Document;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBQA_18 : RavenNewTestBase
    {
        [Fact(Skip = "RavenDB-6314")]
        public void ShouldNotThrowNullReferenceException()
        {
            using (var store = new DocumentStore())
            {
                store.Replication.WaitAsync().Wait(); // should not throw
            }
        }
    }
}
