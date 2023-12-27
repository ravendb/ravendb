#if ALPHABETIC_TEST_RUN

[assembly: Xunit.TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
[assembly: Xunit.TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]

#endif

#if TESTING

#if !ALPHABETIC_TEST_RUN

    [assembly: Xunit.TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]
    [assembly: Xunit.TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer", "Tests.Infrastructure")]

#endif

[assembly: Xunit.TestFramework("Tests.Infrastructure.XunitExtensions.PerformanceTestFramework", "Tests.Infrastructure")]

#endif
