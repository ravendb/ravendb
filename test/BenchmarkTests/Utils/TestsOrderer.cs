using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace BenchmarkTests.Utils
{
    public class TestsOrderer : ITestCaseOrderer, ITestCollectionOrderer
    {
        private static readonly string InitTestName = $"{typeof(Init).FullName}.{nameof(Init.Initialize)}";

        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases) where TTestCase : ITestCase
        {
            var initTest = testCases.SingleOrDefault(x => x.DisplayName == InitTestName);

            if (initTest != null)
                yield return initTest;

            foreach (var test in testCases.OrderBy(x => x.DisplayName))
            {
                if (test.DisplayName == initTest?.DisplayName)
                    continue;

                yield return test;
            }
        }

        public IEnumerable<ITestCollection> OrderTestCollections(IEnumerable<ITestCollection> testCollections)
        {
            var initCollection = testCollections.SingleOrDefault(x => x.DisplayName == nameof(Init));

            if (initCollection != null)
                yield return initCollection;

            foreach (var collection in testCollections.OrderBy(x => x.DisplayName))
            {
                if (collection.DisplayName == initCollection?.DisplayName)
                    continue;

                yield return collection;
            }
        }
    }
}
