using System;
using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using MvcMusicStore.Services;
using MvcMusicStore.ViewModels;
using System.Collections.Generic;
using System.Collections.Specialized;
using Raven.Client;
using MvcMusicStore;

namespace MvcMusicStore.Controllers
{
    public class ShoppingCartController : Controller
    {
        private IDocumentSession session = MvcApplication.CurrentSession;

        

        //
        // GET: /ShoppingCart/

        public ActionResult Index()
        {
            var cart = ShoppingCartFinder.FindShoppingCart();

            // Set up our ViewModel
            var viewModel = new ShoppingCartViewModel
            {
                CartItems = cart.Lines,
                CartTotal = cart.Lines.Count
            };

            // Return the view
            return View(viewModel);
        }

        //
        // GET: /Store/AddToCart/5

        public ActionResult AddToCart(string album)
        {
            var shoppingCart = ShoppingCartFinder.FindShoppingCart();
            shoppingCart.AddToCart(session.Load<Album>(album));
            session.SaveChanges();

            // Go back to the main store page for more shopping
            return RedirectToAction("Index");
        }

        //
        // AJAX: /ShoppingCart/RemoveFromCart/5

        [HttpPost]
        public ActionResult RemoveFromCart(string album)
        {
            // Remove the item from the cart
            var shoppingCart = ShoppingCartFinder.FindShoppingCart();
            string title = shoppingCart.RemoveFromCart(album);

            string message = title == null
                          ? "The album was not found in your shopping cart."
                          : Server.HtmlEncode(title) + " has been removed from your shopping cart.";

            // Display the confirmation message

            return Json(new ShoppingCartRemoveViewModel
            {
                Message = message,
                CartTotal = shoppingCart.Total,
                CartCount = shoppingCart.Lines.Count,
                DeleteId = album
            });
        }

        //
        // GET: /ShoppingCart/CartSummary

        [ChildActionOnly]
        public ActionResult CartSummary()
        {
            var shoppingCart = ShoppingCartFinder.FindShoppingCart();

            ViewData["CartCount"] = shoppingCart.Lines.Count;

            return PartialView("CartSummary");
        }
    }
}