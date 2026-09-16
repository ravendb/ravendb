using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure
{
    public class AlphabeticTestsOrderer : ITestCaseOrderer, ITestCollectionOrderer, ITestClassOrderer, ITestMethodOrderer
    {
        public IReadOnlyCollection<TTestCase> OrderTestCases<TTestCase>(IReadOnlyCollection<TTestCase> testCases) where TTestCase : ITestCase
            => testCases.OrderBy(x => x.TestCaseDisplayName, StringComparer.Ordinal).ToArray();

        public IReadOnlyCollection<TTestCollection> OrderTestCollections<TTestCollection>(IReadOnlyCollection<TTestCollection> testCollections) where TTestCollection : ITestCollection
            => testCollections.OrderBy(x => x.TestCollectionDisplayName, StringComparer.Ordinal).ToArray();

        public IReadOnlyCollection<TTestClass> OrderTestClasses<TTestClass>(IReadOnlyCollection<TTestClass> testClasses) where TTestClass : ITestClass
            => testClasses.OrderBy(x => x.TestClassName, StringComparer.Ordinal).ToArray();

        public IReadOnlyCollection<TTestMethod> OrderTestMethods<TTestMethod>(IReadOnlyCollection<TTestMethod> testMethods) where TTestMethod : ITestMethod
            => testMethods.OrderBy(x => x.MethodName, StringComparer.Ordinal).ToArray();
    }
}
