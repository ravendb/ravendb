// -----------------------------------------------------------------------
//  <copyright file="Wallace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Wallace : RavenTestBase
    {
        [Fact]
        public void CanGetProperErrorFromComputedOrderBy()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var argumentException = Assert.Throws<ArgumentException>(() => session.Query<Order>().OrderBy(x => x.OrderLines.Last().Quantity).ToList());

                    Assert.Equal("Could not understand expression: .OrderBy(x => x.OrderLines.Last().Quantity)", argumentException.Message);
                    Assert.Equal("Not supported computation: x.OrderLines.Last().Quantity. You cannot use computation in RavenDB queries (only simple member expressions are allowed).",
                        argumentException.InnerException.Message);
                }
            }
        }

        private class Order
        {
            public DateTime CreatedOn { get; set; }

            public OrderLineCollection OrderLines { get; set; }


            public Order()
            {
                CreatedOn = DateTime.UtcNow;
                OrderLines = new OrderLineCollection();
            }
        }

        private class OrderLine
        {
            public DateTime CreatedOn { get; set; }
            public ProductReference Product { get; set; }
            public int Quantity { get; set; }
            public List<string> Properties { get; set; }
            public int Id { get; set; }

            public OrderLine()
            {
                CreatedOn = DateTime.UtcNow;
                Properties = new List<string>();
            }
        }

        private class OrderLineCollection : Collection<OrderLine>
        {
            protected override void InsertItem(int index, OrderLine orderLine)
            {
                if (index < Items.Count)
                    throw new ArgumentOutOfRangeException("orderLine", "Orderlines can only be appended");
                VerifyOrderLine(orderLine);
                orderLine.Id = Items == null || Items.Count == 0 ? 0 : Items.Max(o => o.Id) + 1;
                base.InsertItem(index, orderLine);
            }

            protected override void SetItem(int index, OrderLine orderLine)
            {
                if (Items[index].Id != orderLine.Id)
                {
                    throw new ArgumentException("Orderline id cannot be changed");
                }
                VerifyOrderLine(orderLine);
                base.SetItem(index, orderLine);
            }

            private void VerifyOrderLine(OrderLine orderLine)
            {
                if (orderLine == null)
                    throw new ArgumentNullException("orderLine");
                if (orderLine.Product == null)
                    throw new ArgumentException("Orderline with null product", "orderLine");
                if (string.IsNullOrEmpty(orderLine.Product.Id))
                    throw new ArgumentException("Product with empty Id", "orderLine");
                if (string.IsNullOrEmpty(orderLine.Product.CategoryId))
                    throw new ArgumentException(string.Format("Product {0} with empty CategoryId", orderLine.Product.Id), "orderLine");
            }
        }

        private class ProductReference
        {
            public string Id { get; set; }
            public string CategoryId { get; set; }

            public override bool Equals(object o)
            {
                var p = o as ProductReference;
                if (p == null) return false;
                return Id.Equals(p.Id);
            }

            public override int GetHashCode()
            {
                return Id.GetHashCode();
            }
        }
    }
}
