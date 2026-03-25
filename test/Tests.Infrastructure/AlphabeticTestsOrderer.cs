using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure
{
    public class AlphabeticTestsOrderer : ITestCaseOrderer, ITestCollectionOrderer
    {
        public IReadOnlyCollection<TTestCase> OrderTestCases<TTestCase>(IReadOnlyCollection<TTestCase> testCases) where TTestCase : ITestCase
            => testCases.OrderBy(x => x.TestCaseDisplayName, StringComparer.Ordinal).ToArray();

        public IReadOnlyCollection<TTestCollection> OrderTestCollections<TTestCollection>(IReadOnlyCollection<TTestCollection> testCollections) where TTestCollection : ITestCollection
            => testCollections.OrderBy(x => x.TestCollectionDisplayName, StringComparer.Ordinal).ToArray();
    }
}
