using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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
            {
                SlowTests.Issues.RavenDB_15568.SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors(store, Indexes,
                    simpleMapErrors =>
                    {
                        Assert.Equal(1, simpleMapErrors.Errors.Length);
                        Assert.True(simpleMapErrors.Errors.All(x =>
                            x.Error.Contains("that is neither indexed nor stored is useless because it cannot be searched or retrieved.")));
                    });
            }
        }
    }
}
