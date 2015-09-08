using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Storage.Voron.Impl;
using Raven.Tests.MailingList;

namespace ConsoleApplication4
{
    class Program
    {
        public class Item
        {
            public int Number;
        }
        private static void Main(string[] args)
        {
			var transformResults = @"results.Select(order => new {
    order = order,
    merchantOrders = order.MerchantOrders.Select((((Func < dynamic, dynamic > )(LoadDocument))))
}).Select(this0 => new {
    Id = this0.order.Id,
    MerchantOrders = this0.merchantOrders.Select(mo => new {
        mo = mo,
        merchant = this.LoadDocument(mo.MerchantId)
    }).Select(this0 => new {
        MerchantId = this0.merchant.Id,
        MerchantName = this0.merchant.Name,
        MerchantOrderId = this0.mo.Id,
        Items = this0.mo.Items.Select(i => new {
            Id = i.Id,
            Name = i.Name
        })
    })
})";
			Console.WriteLine(new BadTransformer.UserOrderSummaryTransformer().CreateTransformerDefinition(true).TransformResults == transformResults);
			Console.WriteLine(new BadTransformer.UserOrderSummaryTransformer().CreateTransformerDefinition(true).TransformResults);
			Console.WriteLine(IndexPrettyPrinter.TryFormat(transformResults));



        }

    }

    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }

        public string Phone { get; set; }
        public string Fax { get; set; }
    }
}
