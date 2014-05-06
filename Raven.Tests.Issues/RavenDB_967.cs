// -----------------------------------------------------------------------
//  <copyright file="RavenDB_967.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Indexes;
using Raven.Smuggler;
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

		public class ProductWithQueryInput : AbstractTransformerCreationTask<Product>
		{
			public class Result
			{
				public string ProductId { get; set; }
				public string ProductName { get; set; }
				public string Input { get; set; }
			}
			public ProductWithQueryInput()
			{
				TransformResults = docs => from product in docs
										   select new
										   {
											   ProductId = product.Id,
											   ProductName = product.Name,
											   Input = Query("input")
										   };
			}
		}

		[Fact]
		public void CanExportImportTransformers()
		{
		    var file = Path.GetTempFileName();

			try
			{
				using (var documentStore = NewRemoteDocumentStore())
				{
					new ProductWithQueryInput().Execute(documentStore);

					var smugglerApi = new SmugglerApi();

					smugglerApi.ExportData(new SmugglerExportOptions
					{
						ToFile = file,
						From = new RavenConnectionStringOptions
						{
							Url = documentStore.Url,
							DefaultDatabase = documentStore.DefaultDatabase
						}
					}, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
				}

				using (var documentStore = NewRemoteDocumentStore())
				{
					var smugglerApi = new SmugglerApi();

					smugglerApi.ImportData(new SmugglerImportOptions
					{
						FromFile = file,
						To = new RavenConnectionStringOptions
						{
							Url = documentStore.Url,
							DefaultDatabase = documentStore.DefaultDatabase
						}
					}, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));

					var transformers = documentStore.DatabaseCommands.GetTransformers(0, 128);

					Assert.NotNull(transformers);
					Assert.Equal(1, transformers.Length);
					Assert.Equal("ProductWithQueryInput", transformers[0].Name);
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