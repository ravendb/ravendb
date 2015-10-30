// -----------------------------------------------------------------------
//  <copyright file="RavenDB_967.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Client.Indexes;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_967 : RavenTest
    {
        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class ProductWithTransformerParameters : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string ProductId { get; set; }
                public string ProductName { get; set; }
                public string Input { get; set; }
            }
            public ProductWithTransformerParameters()
            {
                TransformResults = docs => from product in docs
                                           select new
                                           {
                                               ProductId = product.Id,
                                               ProductName = product.Name,
                                               Input = Parameter("input")
                                           };
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanExportImportTransformers()
        {
            var file = Path.GetTempFileName();

            try
            {
                using (var documentStore = NewRemoteDocumentStore())
                {
                    new ProductWithTransformerParameters().Execute(documentStore);

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = documentStore.Url,
                            Database = documentStore.DefaultDatabase
                        }),
                        new DatabaseSmugglerFileDestination(file));

                    smuggler.Execute();
                }

                using (var documentStore = NewRemoteDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerFileSource(file),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = documentStore.Url,
                            Database = documentStore.DefaultDatabase
                        }));

                    smuggler.Execute();

                    var transformers = documentStore.DatabaseCommands.GetTransformers(0, 128);

                    Assert.NotNull(transformers);
                    Assert.Equal(1, transformers.Length);
                    Assert.Equal("ProductWithTransformerParameters", transformers[0].Name);
                }
            }
            finally
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
        }
    }
}
