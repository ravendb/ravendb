// -----------------------------------------------------------------------
//  <copyright file="InflectorTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Client.Util;
using Xunit;

namespace SlowTests.Tests
{
    public class InflectorTests : RavenNewTestBase
    {
        [Theory]
        [InlineData("User", "Users")]
        [InlineData("Users", "Users")]
        [InlineData("tanimport", "tanimports")]
        [InlineData("tanimports", "tanimports")]
        public void CanUsePluralizeSafelyOnMaybeAlreadyPluralizedWords(string word, string expected)
        {
            Assert.Equal(expected, Inflector.Pluralize(word));
        }
    }
}
