using FastTests;
using Raven.Server.Web;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Utils
{
    public class HtmlUtilTests : NoDisposalNeeded
    {
        public HtmlUtilTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RenderUnsafePage()
        {
            var html = HtmlUtil.RenderUnsafePage();
            Assert.False(html.Contains("{{"));
            Assert.False(html.Contains("}}"));
        }
    }
}
