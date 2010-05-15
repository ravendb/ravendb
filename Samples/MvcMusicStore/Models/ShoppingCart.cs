using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MvcMusicStore.Models
{
    public class ShoppingCart
    {
        public string Id { get; set; }
        public List<ShoppingCartLine> Lines { get; set; }

        public decimal Total
        {
            // this executes in memory, no database queries here!
            get { return Lines.Sum(x => x.Price); }
        }

        public ShoppingCart()
        {
            Lines = new List<ShoppingCartLine>();
        }

        public class ShoppingCartLine
        {
            public int Quantity { get; set; }
            public decimal Price { get; set; }
            public DateTime DateCreated { get; set; }

            public ShoppingCartLineAlbum Album
            {
                get;
                set;
            }

            public class ShoppingCartLineAlbum
            {
                public string Id { get; set; }
                public string Title { get; set; }
            }
        }

        public void AddToCart(Album album)
        {
            // this runs in memory, there is no database queries here, mister!
            var albumLine = Lines.FirstOrDefault(line => line.Album.Id == album.Id);
            if (albumLine != null)
            {
                albumLine.Quantity++;
                return;
            }
            Lines.Add(new ShoppingCartLine
            {
                Album = new ShoppingCartLine.ShoppingCartLineAlbum
                {
                    Id = album.Id,
                    Title = album.Title
                },
                DateCreated = DateTime.Now,
                Price = album.Price,
                Quantity = 1
            });
        }

        public string RemoveFromCart(string album)
        {
            // this runs in memory, there is no database queries here, mister!
            var albumLine = Lines.FirstOrDefault(line => line.Album.Id == album);
            if (albumLine == null)
                return null;
            albumLine.Quantity--;
            if (albumLine.Quantity == 0)
                Lines.Remove(albumLine);
            return albumLine.Album.Title;
        }

        public ShoppingCart MigrateCart(string newShoppingCartId)
        {
            return new ShoppingCart
            {
                Id = newShoppingCartId,
                Lines = Lines
            };
        }

        public void CreateOrder(Order order)
        {
            order.Lines = new List<Order.OrderLine>(
                Lines.Select(line => new Order.OrderLine
                {
                    Album = new Order.OrderLine.OrderAlbum
                    {
                        Id = line.Album.Id,
                        Title = line.Album.Title
                    },
                    Price = line.Price,
                    Quantity = line.Quantity
                }));
            Lines.Clear();
        }
    }
}