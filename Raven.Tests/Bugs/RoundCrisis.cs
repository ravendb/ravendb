// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class RoundCrisis : IDisposable
	{
		private readonly DocumentStore _documentStore;

		public RoundCrisis()
		{
			_documentStore = new EmbeddableDocumentStore {RunInMemory = true};
			_documentStore.Initialize();
			IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(PriceDocuments_ByDateBySource))), _documentStore);
		}

		[Fact]
		public void When_running_indexByDateBySource_2_doc_available_same_date_same_source_Then_returns_1_document()
		{
			var priceId = Guid.NewGuid();
			CreateAndStoreProductPriceWithSameDateAndSameSource(priceId);
			using (var storeSession = _documentStore.OpenSession())
			{
				var productPriceDocuments = storeSession.Query<PriceDocument, PriceDocuments_ByDateBySource>()
					.Customize(x=>x.WaitForNonStaleResults())
					.Where(x => x.PriceId == priceId);
				Assert.Equal(1, productPriceDocuments.Count());
			}
		}

		/*
			 DocumentId				price    source  date                                   version
				1                     0.85      Bla      2011-09-01:08:00:000          1
				2                     0.95      Bla      2011-09-01:09:00:000          2
				3                     1.05      Foo      2011-09-01:08:00:000          3
				4                     0.85      Foo      2011-09-02:08:00:000          1
			 */

		[Fact]
		public void When_running_indexByDateBySource_with_scenario_in_comment_Then_returns_3_documents()
		{
			var priceId = Guid.NewGuid();
			CreateAndStorePriceWithWithDatesAndSources(priceId);
			using (var storeSession = _documentStore.OpenSession())
			{
				var productPriceDocuments = storeSession.Query<PriceDocument, PriceDocuments_ByDateBySource>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.PriceId == priceId);
				Assert.Equal(3, productPriceDocuments.Count());
			}
		}


		private void CreateAndStorePriceWithWithDatesAndSources(Guid productId)
		{
			using (var storeSession = _documentStore.OpenSession())
			{
				var sourceBla = "Bla";
				var sourceFoo = "Foo";
				var pricingDate = new DateTime(2011, 9, 1, 8, 0, 0);
				var productPriceDocument1 = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/1",
					PricingDate = pricingDate,
					PriceId = productId,
					Source = sourceBla,
					Version = 1
				};
				var productPriceDocument2 = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/2",
					PricingDate = pricingDate.AddHours(1),
					PriceId = productId,
					Source = sourceBla,
					Version = 2
				};
				storeSession.Store(productPriceDocument1);
				storeSession.Store(productPriceDocument2);
				var productPriceDocument3 = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/3",
					PricingDate = pricingDate,
					PriceId = productId,
					Source = sourceFoo,
					Version = 3
				};
				var productPriceDocument4 = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/4",
					PricingDate = pricingDate.AddDays(1),
					PriceId = productId,
					Source = sourceFoo,
					Version = 4
				};
				storeSession.Store(productPriceDocument3);
				storeSession.Store(productPriceDocument4);

				storeSession.SaveChanges();
			}
		}

		private void CreateAndStoreProductPriceWithSameDateAndSameSource(Guid productId)
		{
			using (var storeSession = _documentStore.OpenSession())
			{
				var moody = "Moody";
				var pricingDate = new DateTime(2011, 4, 1, 8, 0, 0);
				var productPriceDocument = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/1",
					PricingDate = pricingDate,
					PriceId = productId,
					Source = moody,
					Version = 1
				};
				var productPriceDocument1 = new PriceDocument
				{
					AskPrice = 1m,
					BidPrice = 1m,
					Id = productId + "/2",
					PricingDate = new DateTime(2011, 4, 1, 9, 0, 0),
					PriceId = productId,
					Source = moody,
					Version = 2
				};
				storeSession.Store(productPriceDocument);
				storeSession.Store(productPriceDocument1);
				storeSession.SaveChanges();
			}
		}

		#region Nested type: PriceDocument

		public class PriceDocument
		{
			public Guid PriceId { get; set; }

			public decimal AskPrice { get; set; }

			public decimal BidPrice { get; set; }

			public DateTime PricingDate { get; set; }

			public string Source { get; set; }

			public int Version { get; set; }

			public string Id { get; set; }
		}

		#endregion

		#region Nested type: PriceDocuments_ByDateBySource

		public class PriceDocuments_ByDateBySource : AbstractIndexCreationTask<PriceDocument>
		{
			public PriceDocuments_ByDateBySource()
			{
				Map = docs => from priceDocument in docs
				              select new
				              {
				              	PricingDate =
				              	new DateTime(priceDocument.PricingDate.Year, priceDocument.PricingDate.Month,
				              	             priceDocument.PricingDate.Day),
				              	priceDocument.Source,
				              	priceDocument.PriceId,
				              	priceDocument.AskPrice,
				              	priceDocument.BidPrice,
								priceDocument.Version
				              };
				Reduce = results => from result in results
				                    group result by new
				                    {
				                    	result.PricingDate,
				                    	result.Source,
				                    }
				                    into price
				                    let lastPrice = price.OrderByDescending(p => p.Version).First()
				                    select new
				                    {
				                    	lastPrice.PriceId,
				                    	lastPrice.PricingDate,
				                    	lastPrice.AskPrice,
				                    	lastPrice.BidPrice,
				                    	lastPrice.Source,
				                    	lastPrice.Version
				                    };
			}
		}

		#endregion

		
		public void Dispose()
		{
			_documentStore.Dispose();
		}
	}
}

