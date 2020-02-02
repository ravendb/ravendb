using Xunit;

#if TESTING

[assembly: TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: TestFramework("Tests.Infrastructure.XunitExtensions.PerformanceTestFramework", "Tests.Infrastructure")]

#endif
