using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure
{

    public class AlphabeticTestsOrderer : ITestCaseOrderer, ITestCollectionOrderer
    {
        public AlphabeticTestsOrderer(IMessageSink diagnosticMessageSink)
        {
        }

        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases) where TTestCase : ITestCase
            => testCases.OrderBy(x => x.DisplayName, StringComparer.Ordinal);

        public IEnumerable<ITestCollection> OrderTestCollections(IEnumerable<ITestCollection> testCollections)
            => testCollections.OrderBy(x => x.DisplayName, StringComparer.Ordinal);
    }
}
