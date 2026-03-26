using FastTests;
using Tests.Infrastructure;
using static SlowTests.Issues.RavenDB_15568;
using Xunit;

namespace StressTests.Issues
{
    public class RavenDB_15568_Stress : RavenTestBase
    {
        public RavenDB_15568_Stress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInCorax(Options options)
        {
            using (var store = GetDocumentStore(options))
                SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInternal<SimpleMapIndexWithDefaultFields>(store, Indexes, _coraxAssertion);
        }
    }
}
