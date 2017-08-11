using FastTests;
using Raven.Server.Web;
using Xunit;

namespace SlowTests.Utils
{
    public class HtmlUtilTests : NoDisposalNeeded
    {
        [Fact]
        public void RenderUnsafePage()
        {
            var html = HtmlUtil.RenderUnsafePage();
            Assert.False(html.Contains("{{"));
            Assert.False(html.Contains("}}"));
        }
    }
}
