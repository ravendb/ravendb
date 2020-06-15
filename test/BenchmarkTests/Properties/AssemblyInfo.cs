using Xunit;

[assembly: TestCaseOrderer("BenchmarkTests.Utils.TestsOrderer", "BenchmarkTests")]
[assembly: TestCollectionOrderer("BenchmarkTests.Utils.TestsOrderer", "BenchmarkTests")]
[assembly: TestFramework("Tests.Infrastructure.XunitExtensions.PerformanceTestFramework", "Tests.Infrastructure")]
