using Xunit;

[assembly: TestCaseOrderer(typeof(BenchmarkTests.Utils.TestsOrderer))]
[assembly: TestCollectionOrderer(typeof(BenchmarkTests.Utils.TestsOrderer))]
[assembly: TestFramework(typeof(Tests.Infrastructure.XunitExtensions.PerformanceTestFramework))]
