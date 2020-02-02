using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    
    public class AlphabeticTestsOrderer : ITestCaseOrderer, ITestCollectionOrderer
    {
        private readonly IMessageSink _diagnosticMessageSink;

        public AlphabeticTestsOrderer(IMessageSink diagnosticMessageSink) => 
            _diagnosticMessageSink = diagnosticMessageSink;

        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases) where TTestCase : ITestCase
        {
            var orderedTestCases = testCases.OrderBy(x => x.DisplayName).ToList();

            if (_diagnosticMessageSink != null) //just in case
            {
                var message = new DiagnosticMessage("Alphabetically ordered {0} test cases", orderedTestCases.Count);
                _diagnosticMessageSink.OnMessage(message);
            }

            return orderedTestCases;
        }

        public IEnumerable<ITestCollection> OrderTestCollections(IEnumerable<ITestCollection> testCollections)
            => testCollections.OrderBy(x => x.DisplayName);
    }
}
