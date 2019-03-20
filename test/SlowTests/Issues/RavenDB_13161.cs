using FastTests;
using Raven.Server.Documents.Indexes;
using Raven.Server.Web.System;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13161 : RavenTestBase
    {
        [Fact]
        public void DatabaseNameWithDocsValidation()
        {
            Assert.False(ResourceNameValidator.IsValidResourceName(".", null, out _));
            Assert.False(ResourceNameValidator.IsValidResourceName("..", null, out _));
            Assert.False(ResourceNameValidator.IsValidResourceName("...", null, out _));

            Assert.True(ResourceNameValidator.IsValidResourceName(".a", null, out _));
            Assert.True(ResourceNameValidator.IsValidResourceName(".4..", null, out _));
        }

        [Fact]
        public void IndexNameWithDocsValidation()
        {
            Assert.False(IndexStore.IsValidIndexName(".", true, out _));
            Assert.False(IndexStore.IsValidIndexName("..", true, out _));
            Assert.False(IndexStore.IsValidIndexName("...", true, out _));

            Assert.True(IndexStore.IsValidIndexName(".a", true, out _));
            Assert.True(IndexStore.IsValidIndexName(".4..", true, out _));
        }
    }
}
