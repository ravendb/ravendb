#if ALPHABETIC_TEST_RUN

[assembly: Xunit.TestCaseOrderer(typeof(Tests.Infrastructure.AlphabeticTestsOrderer))]
[assembly: Xunit.TestCollectionOrderer(typeof(Tests.Infrastructure.AlphabeticTestsOrderer))]

#endif

#if TESTING

#if !ALPHABETIC_TEST_RUN

    [assembly: Xunit.TestCaseOrderer(typeof(Tests.Infrastructure.AlphabeticTestsOrderer))]
    [assembly: Xunit.TestCollectionOrderer(typeof(Tests.Infrastructure.AlphabeticTestsOrderer))]

#endif

[assembly: Xunit.TestFramework(typeof(Tests.Infrastructure.XunitExtensions.PerformanceTestFramework))]

#endif
