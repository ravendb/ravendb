using Raven.Abstractions;
using Raven.Database.Linq;
using System.Linq;
using System.Collections.Generic;
using System.Collections;
using System;
using Raven.Database.Linq.PrivateExtensions;
using Lucene.Net.Documents;
using System.Globalization;
using Raven.Database.Indexing;


public class Index_Sales_2fByLocation : Raven.Database.Linq.AbstractViewGenerator
{
	public Index_Sales_2fByLocation()
	{
		this.ViewText = @"docs.Sales.Select(sale => new {
    _ = (object) null,
    SaleId = sale.__document_id,
    Locations = Enumerable.ToArray(sale.Locations.Select(l => new {
        Lat = ((double) l.Lat),
        Lng = ((double) l.Lng)
    })),
    TotalSold = 0
})
docs.Orders.Select(order => new {
    _ = (object) null,
    SaleId = order.SaleId,
    Locations = new[] {
        new {
            Lat = 0,
            Lng = 0
        }
    },
    TotalSold = 1
})
results.GroupBy(sitesale => sitesale.SaleId).Select(sales => new {
    sales = sales,
    locations = sales.SelectMany(x => x.Locations)
}).SelectMany(this7 => this7.sales, (this7, sale) => new {
    _ = this7.locations.Select(l => SpatialIndex.Generate(((double) l.Lat), ((double) l.Lng))),
    SaleId = sale.SaleId,
    Locations = this7.locations,
    TotalSold = Enumerable.Sum(this7.sales, x => ((int) x.TotalSold))
})";
		this.ForEntityNames.Add("Sales");
		this.AddMapDefinition(docs => docs.Where(__document => string.Equals(__document["@metadata"]["Raven-Entity-Name"], "Sales", System.StringComparison.InvariantCultureIgnoreCase)).Select((Func<dynamic, dynamic>)(sale => new
		{
			_ = (object)null,
			SaleId = sale.__document_id,
			Locations = Enumerable.ToArray(sale.Locations.Select((Func<dynamic, dynamic>)(l => new
			{
				Lat = ((double)l.Lat),
				Lng = ((double)l.Lng)
			}))),
			TotalSold = 0,
			__document_id = sale.__document_id
		})));
		this.ForEntityNames.Add("Orders");
		this.AddMapDefinition(docs => docs.Where(__document => string.Equals(__document["@metadata"]["Raven-Entity-Name"], "Orders", System.StringComparison.InvariantCultureIgnoreCase)).Select((Func<dynamic, dynamic>)(order => new
		{
			_ = (object)null,
			SaleId = order.SaleId,
			Locations = new[] {
				new {
					Lat = 0,
					Lng = 0
				}
			},
			TotalSold = 1,
			__document_id = order.__document_id
		})));
		this.ReduceDefinition = results => results.GroupBy((Func<dynamic, dynamic>)(sitesale => sitesale.SaleId)).Select((Func<IGrouping<dynamic, dynamic>, dynamic>)(sales => new
		{
			sales = sales,
			locations = sales.SelectMany((Func<dynamic, IEnumerable<dynamic>>)(x => (IEnumerable<dynamic>)(x.Locations)))
		})).SelectMany((Func<dynamic, IEnumerable<dynamic>>)(this7 => (IEnumerable<dynamic>)(this7.sales)), (Func<dynamic, dynamic, dynamic>)((this7, sale) => new
		{
			_ = this7.locations.Select((Func<dynamic, dynamic>)(l => SpatialGenerate(((double)l.Lat), ((double)l.Lng)))),
			SaleId = sale.SaleId,
			Locations = this7.locations,
			TotalSold = Enumerable.Sum(this7.sales, (Func<dynamic, int>)(x => ((int)x.TotalSold)))
		}));
		this.GroupByExtraction = sitesale => sitesale.SaleId;
		this.AddField("_");
		this.AddField("SaleId");
		this.AddField("Locations");
		this.AddField("TotalSold");
	}
}
