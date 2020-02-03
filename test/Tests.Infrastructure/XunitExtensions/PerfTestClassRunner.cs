using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;
using Xunit.Sdk;
using XunitLogger;

namespace Tests.Infrastructure.XunitExtensions
{
    public class PerfTestClassRunner : XunitTestClassRunner
    {
        public PerfTestClassRunner(
            ITestClass testClass, 
            IReflectionTypeInfo @class, 
            IEnumerable<IXunitTestCase> testCases, 
            IMessageSink diagnosticMessageSink, 
            IMessageBus messageBus, 
            ITestCaseOrderer testCaseOrderer, 
            ExceptionAggregator aggregator, 
            CancellationTokenSource cancellationTokenSource, 
            IDictionary<Type, object> collectionFixtureMappings) : base(testClass, @class, testCases, diagnosticMessageSink, messageBus, testCaseOrderer, aggregator, cancellationTokenSource, collectionFixtureMappings)
        {
        }

        protected override Task<RunSummary> RunTestMethodAsync(ITestMethod testMethod, IReflectionMethodInfo method, IEnumerable<IXunitTestCase> testCases, object[] constructorArguments)
        {
            TestResourcesAnalyzer.Start(testMethod);
            return base.RunTestMethodAsync(testMethod, method, testCases, constructorArguments)
                       .ContinueWith(t =>
                       {
                           TestResourcesAnalyzer.End(testMethod);
                           return t.Result;
                       });
        }
    }
}
