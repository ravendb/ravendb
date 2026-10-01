using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Sdk;
using Xunit.v3;

namespace BenchmarkTests.Utils
{
    public class TestsOrderer : ITestCaseOrderer, ITestCollectionOrderer, ITestClassOrderer, ITestMethodOrderer
    {
        private static readonly string InitClassName = typeof(Init).FullName;

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

        public IReadOnlyCollection<TTestClass> OrderTestClasses<TTestClass>(IReadOnlyCollection<TTestClass> testClasses) where TTestClass : ITestClass
        {
            return Hoist(testClasses, x => x.TestClassName, InitClassName);
        }

        public IReadOnlyCollection<TTestMethod> OrderTestMethods<TTestMethod>(IReadOnlyCollection<TTestMethod> testMethods) where TTestMethod : ITestMethod
        {
            return Hoist(testMethods, x => x.MethodName, nameof(Init.Initialize));
        }

        private static IReadOnlyCollection<T> Hoist<T>(IReadOnlyCollection<T> items, Func<T, string> nameSelector, string nameToHoist)
        {
            List<T> result = new List<T>();
            T itemToHoist = items.SingleOrDefault(x => nameSelector(x) == nameToHoist);

            if (itemToHoist != null)
                result.Add(itemToHoist);

            foreach (T item in items.OrderBy(nameSelector))
            {
                if (ReferenceEquals(item, itemToHoist))
                    continue;

                result.Add(item);
            }

            return result;
        }
    }
}
