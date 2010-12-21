//-----------------------------------------------------------------------
// <copyright file="Order.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

namespace MvcMusicStore.Models
{
    public partial class Order
    {
        public Order()
        {
            Lines = new List<OrderLine>();
        }

        public decimal Total
        {
            get
            {
                // this executes in memory, no database queries here!
                return Lines.Sum(x => x.Price);
            }
        }

        public string FirstName { get; set; }
        public string LastName { get; set; }

        public string Address { get; set; }

        public string City { get; set; }

        public string State { get; set; }

        public string PostalCode { get; set; }

        public string Country { get; set; }

        public string Phone { get; set; }

        public string Email { get; set; }

        public DateTime OrderDate { get; set; }

        public string Username { get; set; }

        public List<OrderLine> Lines { get; set; }

        public string Id { get; set; }

        #region Nested type: OrderLine

        public class OrderLine
        {
            public int Quantity { get; set; }
            public decimal Price { get; set; }

            public OrderAlbum Album { get; set; }

            #region Nested type: OrderAlbum

            public class OrderAlbum
            {
                public string Id { get; set; }
                public string Title { get; set; }
            }

            #endregion
        }

        #endregion
    }
}
