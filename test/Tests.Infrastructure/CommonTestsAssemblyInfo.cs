using Xunit;

#if DEBUG || VALIDATE || TEST_PERFORMANCE

[assembly: TestCaseOrderer("Tests.Infrastructure.AlphabeticTestsOrderer","Tests.Infrastructure")]
[assembly: TestCollectionOrderer("Tests.Infrastructure.AlphabeticTestsOrderer","Tests.Infrastructure")]
[assembly: CollectionBehavior(DisableTestParallelization = true)]

#endif
