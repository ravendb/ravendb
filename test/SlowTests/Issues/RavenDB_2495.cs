// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2495.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using FastTests.Client.MoreLikeThis;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2495 : RavenTestBase
    {
        [Fact]
        public void IncludesShouldWorkWithMoreLikeThis()
        {
            using (var x = new MoreLikeThisTests())
            {
                x.IncludesShouldWorkWithMoreLikeThis();
            }
        }
    }
}
