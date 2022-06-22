using Xunit;

#if ALPHABETIC_TEST_RUN

[assembly: TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]

#endif

#if TESTING

    #if !ALPHABETIC_TEST_RUN

    [assembly: TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
    [assembly: TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]

    #endif

[assembly: TestFramework("Tests.Infrastructure.XunitExtensions.PerformanceTestFramework", "Tests.Infrastructure")]

#endif
