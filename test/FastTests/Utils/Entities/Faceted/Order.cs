using System;

namespace SlowTests.Core.Utils.Entities.Faceted
{
    public class Order
    {
        public string Product { get; set; }
        public decimal Total { get; set; }
        public Currency Currency { get; set; }
        public int Quantity { get; set; }
        public long Region { get; set; }
        public DateTime At { get; set; }
        public float Tax { get; set; }
    }
}
