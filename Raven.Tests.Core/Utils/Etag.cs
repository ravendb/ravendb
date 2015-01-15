// -----------------------------------------------------------------------
//  <copyright file="Etag.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Core.Utils
{
    public class EtagTests
    {
        [Fact]
        public void RoundTrip()
        {
            var etag = Etag.Parse("37A7CD39-8013-A184-0044-B382D30A5509");
            Assert.Equal("37A7CD39-8013-A184-0044-B382D30A5509", etag.ToString());
        }
    }
}