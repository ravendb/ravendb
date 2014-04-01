// -----------------------------------------------------------------------
//  <copyright file="CustomizingIndexQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class CustomizingIndexQuery : RavenTest
	{
		[Fact]
		public void CanSkipTransformResults()
		{
			using (var store = NewDocumentStore())
			{
				new LiveProjection.PurchaseHistoryIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new LiveProjection.Shipment
					{
						UserId = "users/ayende",
						Items = new List<LiveProjection.ShipmentItem>
						{
							new LiveProjection.ShipmentItem {ProductId = "products/123"},
							new LiveProjection.ShipmentItem {ProductId = "products/312"},
							new LiveProjection.ShipmentItem {ProductId = "products/243"}
						},
						Destination = new LiveProjection.Address
						{
							Town = "Hadera"
						}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var q = session.Query<LiveProjection.Shipment, LiveProjection.PurchaseHistoryIndex>()
						   .Customize(c => c.WaitForNonStaleResults().BeforeQueryExecution(x => x.SkipTransformResults = true))
						   .Single();

					Assert.Equal("Hadera", q.Destination.Town);
				}
			}
		}

		[Fact]
		public void CanSkipTransformResults_Lucene()
		{
			using (var store = NewDocumentStore())
			{
				new LiveProjection.PurchaseHistoryIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new LiveProjection.Shipment
					{
						UserId = "users/ayende",
						Items = new List<LiveProjection.ShipmentItem>
						{
							new LiveProjection.ShipmentItem {ProductId = "products/123"},
							new LiveProjection.ShipmentItem {ProductId = "products/312"},
							new LiveProjection.ShipmentItem {ProductId = "products/243"}
						},
						Destination = new LiveProjection.Address
						{
							Town = "Hadera"
						}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    var q = session.Advanced.DocumentQuery<LiveProjection.Shipment, LiveProjection.PurchaseHistoryIndex>()
							.WaitForNonStaleResults()
							.BeforeQueryExecution(x => x.SkipTransformResults = true)
						   .Single();

					Assert.Equal("Hadera", q.Destination.Town);
				}

			}
		}
	}
}