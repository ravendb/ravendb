using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Commands;
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

        [Fact]
        public void CompareExchangeKeyValidationWillThrowOnLargeKey()
        {
            DoNotReuseServer();

            var store = GetDocumentStore();
            var ex = Assert.Throws<RavenException>(() => store.Operations.Send(new PutCompareExchangeValueOperation<string>(new string('a', 513), "a", 0)));

            Assert.Contains($" key cannot exceed {AddOrUpdateCompareExchangeCommand.MaxNumberOfCompareExchangeKeyBytes} bytes", ex.Message);
        }
    }
}
