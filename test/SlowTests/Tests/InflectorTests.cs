using FastTests;
using Raven.Client.Documents.Conventions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests
{
    public class InflectorTests : RavenTestBase
    {
        public InflectorTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
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