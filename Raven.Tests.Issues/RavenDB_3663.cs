

using System.Collections.Generic;
using Raven.Database.Util;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3663
    {
        [Fact]
        public void CanPropertyAnalyseBidings()
        {
            AssertBindings(new string[] { }, new List<string>(), new List<string>());
            AssertBindings(new [] { "FirstName", "LastName", "underscore_test"}, new List<string> { "FirstName", "LastName", "underscore_test" }, new List<string>());
            AssertBindings(new [] { "FirstName + ' ' + LastName" }, new List<string>(), new List<string> { "FirstName", "LastName" } );
            AssertBindings(new[] { "FirstName + ' ' + LastName", "LastName", "AnotherProp" }, new List<string> { "AnotherProp" }, new List<string> { "FirstName", "LastName" });
            AssertBindings(new[] { "min(FirstName[0], LastName[2].Address)"}, new List<string>(), new List<string> { "0", "2", "Address","FirstName", "LastName", "min" });
        }

        private static void AssertBindings(string[] input, List<string> expectedSimpleBindings, List<string> expectedCompoundBindings)
        {
            var analyzedBindings = BindingsHelper.AnalyzeBindings(input);
            Assert.Equal(expectedSimpleBindings, analyzedBindings.SimpleBindings);
            Assert.Equal(expectedCompoundBindings, analyzedBindings.CompoundBindings);
        }
    }
}

