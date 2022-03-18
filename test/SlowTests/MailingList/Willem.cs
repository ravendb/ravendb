using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Willem : RavenTestBase
    {
        public Willem(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ThisIsMyTest()
        {
            using (var store = GetDocumentStore())
            {
                new Sales_ByDateProduct().Execute(store);
                using (var session = store.OpenSession())
                {
                    var order = new Order()
                    {
                        TableId = "1",
                        MenuCardId = 34,
                        PaymentType = PaymentType.Cash,
                        CreatedOn = DateTime.Now,
                        State = OrderState.Paid,
                        OrderLines = new OrderLineCollection()
                            {
                                new OrderLine()
                                {
                                    Product = new ProductReference() {CategoryId = "x", Id = "x", Price = 10.0m, Vat = 2.0m},
                                    Course = new Course() {Id = 0},
                                    Guest = new Guest() {Id = 0}
                                }
                            },
                        LocationId = 123

                    };
                    session.Store(order);
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    //  This call generates a "System.FormatException: Input string was not in a correct format."
                    session.Query<Sales_ByDateProduct.ReduceResult, Sales_ByDateProduct>().ToList();

                    //  The index document looks something like this:
                    //  {
                    //   "Date": "2012-12-23T00:00:00.0000000",
                    //   "ProductId": "x",
                    //   "PaymentType": "Cash",
                    //   "LocationId": "34",
                    //   "Amount": "14"
                    //  }

                    //  Why are locationId en Amount stored as string ? Is that part of the problem ?
                    //  What am I doing wrong ?
                }
            }
        }



        //
        //  INDEX
        //

        private class Sales_ByDateProduct : AbstractIndexCreationTask<Order, Sales_ByDateProduct.ReduceResult>
        {
            public class ReduceResult
            {
                public DateTime Date { get; set; }
                public string ProductId { get; set; }
                public decimal Amount { get; set; }
                public PaymentType PaymentType { get; set; }
                public int LocationId { get; set; }
            }

            public Sales_ByDateProduct()
            {
                Map = orders => from o in orders
                                where o.State == OrderState.Paid
                                let p = o.PaymentType
                                let locationId = o.LocationId
                                let createdOn = o.CreatedOn
                                from l in o.OrderLines
                                select new
                                {
                                    Date = createdOn,
                                    ProductId = l.Product.Id,
                                    PaymentType = p,
                                    Amount = l.Product.Price,
                                    LocationId = locationId
                                };
                Reduce = results => results
                    .GroupBy(r => new { r.ProductId, r.Date.Date, r.PaymentType, r.LocationId })
                    .Select(g => new ReduceResult()
                    {
                        Date = g.Key.Date,
                        ProductId = g.Key.ProductId,
                        PaymentType = g.Key.PaymentType,
                        LocationId = g.Key.LocationId,
                        Amount = g.Sum(t => t.Amount)
                    });
            }
        }


        //
        //  MODEL
        //

        private class Order : LocationBaseDocument
        {
            public string TableId { get; set; }
            public int MenuCardId { get; set; }
            public PaymentType PaymentType { get; set; }
            public DateTime PaidOn { get; set; }
            public string Name { get; set; }
            public DateTime CreatedOn { get; set; }
            public OrderState State { get; set; }
            public OrderLineCollection OrderLines { get; set; }
            public CourseCollection Courses { get; set; }
            public GuestCollection Guests { get; set; }

            public Order()
            {
                CreatedOn = DateTime.UtcNow;
                OrderLines = new OrderLineCollection();
                Courses = new CourseCollection();
                Guests = new GuestCollection();
            }

            public override bool Validate()
            {
                return true;
            }


            public override string ToString()
            {
                return "Order #" + Id;
            }
        }
        [JsonObject(IsReference = true)]
        private class Course
        {
            public int Id { get; set; }
            public DateTime? RequestedOn { get; set; }
            public DateTime? ServedOn { get; set; }
        }

        private class CourseCollection : Collection<Course>
        {
            protected override void InsertItem(int index, Course item)
            {
                if (index < Items.Count)
                    throw new ArgumentOutOfRangeException("item", "Course can only be appended");
                item.Id = Items.Count;
                base.InsertItem(index, item);
            }

            protected override void SetItem(int index, Course item)
            {
                for (int i = 0; i < Count; i++)
                    if (i != index && Items[i].Id == item.Id)
                        throw new ArgumentException(string.Format("Course with id {0} already exists", item.Id));
                base.SetItem(index, item);
            }

            public Course GetById(int id)
            {
                return Items.FirstOrDefault(l => l.Id == id);
            }

            protected override void RemoveItem(int index)
            {
                base.RemoveItem(index);
                for (int i = index; i < Count; i++)
                    Items[i].Id--;
            }
        }
        [JsonObject(IsReference = true)]
        private class Guest
        {
            public int Id { get; set; }
            public int Diet { get; set; }
            public bool IsHost { get; set; }
            public Gender Gender { get; set; }
        }
        private class GuestCollection : Collection<Guest>
        {
            protected override void InsertItem(int index, Guest item)
            {
                if (Items.Any(g => g.Id == item.Id))
                    throw new ArgumentException(string.Format("Guest with id {0} already exists", item.Id));
                base.InsertItem(index, item);
            }

            protected override void SetItem(int index, Guest item)
            {
                for (int i = 0; i < Count; i++)
                    if (i != index && Items[i].Id == item.Id)
                        throw new ArgumentException(string.Format("Guest with id {0} already exists", item.Id));

                base.SetItem(index, item);
            }

            public Guest GetById(int id)
            {
                return Items.FirstOrDefault(l => l.Id == id);
            }
        }
        [JsonObject(IsReference = true)]
        private class OrderLine
        {
            public DateTime CreatedOn { get; set; }
            public ProductReference Product { get; set; }
            public int Quantity { get; set; }
            public string Note { get; set; }
            public Course Course { get; set; }
            public Guest Guest { get; set; }
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

            public OrderLine GetById(int id)
            {
                return Items.FirstOrDefault(l => l.Id == id);
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

            public decimal Price { get; set; }
            public decimal Vat { get; set; }

            public string CategoryId { get; set; }

            public bool IsFood { get; set; }

            public ProductReference()
            {
            }

            //public ProductReference(Product product, Category category)
            //{
            //    Id = product.Key;
            //    Price = product.Price;
            //    Vat = (product.Vat.Percentage/(100 + product.Vat.Percentage))*Price;
            //    CategoryId = category.Name;
            //    IsFood = category.IsFood;
            //}

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
        private enum OrderState
        {
            Reserved,
            Started,
            Ordering,
            Billed,
            Paid
        }

        private enum PaymentType
        {
            Unpaid,
            Cash,
            Pin,
            CreditCard
        }

        private enum Gender
        {
            Male,
            Female
        }

        private abstract class LocationBaseDocument : BaseDocument
        {
            public int LocationId { get; set; }
        }

        private abstract class CompanyBaseDocument : BaseDocument
        {
            public int CompanyId { get; set; }
        }

        private abstract class BaseDocument
        {
            public string Id { get; set; }
            public abstract bool Validate();
        }
    }
}
