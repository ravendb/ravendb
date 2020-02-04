using Xunit;

//TODO: remove DEBUG when HRINT-1503 is finished
#if DEBUG || TESTING

[assembly: TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: TestFramework("Tests.Infrastructure.XunitExtensions.PerfTestFramework", "Tests.Infrastructure")]

#endif
