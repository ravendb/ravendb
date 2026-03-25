using System.Collections.Generic;
using System.Linq;
using Xunit.Sdk;
using Xunit.v3;

namespace BenchmarkTests.Utils
{
    public class TestsOrderer : ITestCaseOrderer, ITestCollectionOrderer
    {
        private static readonly string InitTestName = $"{typeof(Init).FullName}.{nameof(Init.Initialize)}";

        public IReadOnlyCollection<TTestCase> OrderTestCases<TTestCase>(IReadOnlyCollection<TTestCase> testCases) where TTestCase : ITestCase
        {
            var result = new List<TTestCase>();
            var initTest = testCases.SingleOrDefault(x => x.TestCaseDisplayName == InitTestName);

            if (initTest != null)
                result.Add(initTest);

            foreach (var test in testCases.OrderBy(x => x.TestCaseDisplayName))
            {
                if (test.TestCaseDisplayName == initTest?.TestCaseDisplayName)
                    continue;

                result.Add(test);
            }

            return result;
        }

        public IReadOnlyCollection<TTestCollection> OrderTestCollections<TTestCollection>(IReadOnlyCollection<TTestCollection> testCollections) where TTestCollection : ITestCollection
        {
            var result = new List<TTestCollection>();
            var initCollection = testCollections.SingleOrDefault(x => x.TestCollectionDisplayName == nameof(Init));

            if (initCollection != null)
                result.Add(initCollection);

            foreach (var collection in testCollections.OrderBy(x => x.TestCollectionDisplayName))
            {
                if (collection.TestCollectionDisplayName == initCollection?.TestCollectionDisplayName)
                    continue;

                result.Add(collection);
            }

            return result;
        }
    }
}
