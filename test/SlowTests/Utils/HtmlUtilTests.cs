using FastTests;
using Raven.Server.Web;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Utils
{
    public class HtmlUtilTests : NoDisposalNeeded
    {
        public HtmlUtilTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void RenderUnsafePage()
        {
            var html = HtmlUtil.RenderUnsafePage();
            Assert.False(html.Contains("{{"));
            Assert.False(html.Contains("}}"));
        }
    }
}
