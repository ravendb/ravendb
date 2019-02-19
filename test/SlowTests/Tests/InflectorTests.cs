// -----------------------------------------------------------------------
//  <copyright file="InflectorTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Conventions;
using Xunit;

namespace SlowTests.Tests
{
    public class InflectorTests : RavenTestBase
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
