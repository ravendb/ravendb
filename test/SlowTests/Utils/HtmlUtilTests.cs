using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Web;
using Xunit;

namespace SlowTests.Utils
{
    public class HtmlUtilTests
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
