//-----------------------------------------------------------------------
// <copyright file="ShoppingCartFinder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Web;
using MvcMusicStore.Models;

namespace MvcMusicStore.Services
{
    public static class ShoppingCartFinder
    {
        public const string CartSessionKey = "CartId";

        private const string ShoppingCartPrefix = "shoppingcarts/";

        public static ShoppingCart FindShoppingCart()
        {
            var shoppingCartId = GetShoppingCartId(HttpContext.Current);
            var shoppingCart = MvcApplication.CurrentSession.Load<ShoppingCart>(shoppingCartId);
            if(shoppingCart == null)
            {
                shoppingCart = new ShoppingCart {Id = shoppingCartId};
                MvcApplication.CurrentSession.Store(shoppingCart);// in memory operation only
            }
            return shoppingCart;
        }

        public static string SetShoppingCartId(string userName)
        {
            var id = ShoppingCartPrefix +userName;
            HttpContext.Current.Session[CartSessionKey] = id;
            return id;
        }

        private static string GetShoppingCartId( HttpContext context)
        {
            if (context.Session[CartSessionKey] == null)
            {
                if (!string.IsNullOrWhiteSpace(context.User.Identity.Name))
                {
                    // User is logged in, associate the cart with there username
                    context.Session[CartSessionKey] = ShoppingCartPrefix + context.User.Identity.Name;
                }
                else
                {
                    // Generate a new random GUID using System.Guid Class
                    Guid tempCartId = Guid.NewGuid();

                    // Send tempCartId back to client as a cookie
                    context.Session[CartSessionKey] = ShoppingCartPrefix + tempCartId;
                }
            }
            return context.Session[CartSessionKey].ToString();
        }
    }
}
