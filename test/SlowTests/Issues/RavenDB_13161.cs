﻿using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
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

        [Fact]
        public void CannotPutIndexWithNameClash()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefBuilder1 = new IndexDefinitionBuilder<Order>()
                {
                    Map = docs => from doc in docs select new { Index = doc.Company },
                };

                var indexDefinition1 = indexDefBuilder1.ToIndexDefinition(store.Conventions);
                indexDefinition1.Name = "Index_With_Name_Clash";

                var indexDefBuilder2 = new IndexDefinitionBuilder<Order>()
                {
                    Map = docs => from doc in docs select new { Index = doc.Company },
                };

                var indexDefinition2 = indexDefBuilder2.ToIndexDefinition(store.Conventions);
                indexDefinition2.Name = "Index/With/Name/Clash";

                Assert.Throws<IndexCreationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition1, indexDefinition2)));
            }
        }
    }
}
