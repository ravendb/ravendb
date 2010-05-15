using System;
using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using MvcMusicStore.Services;
using MvcMusicStore.ViewModels;
using Raven.Client;

namespace MvcMusicStore.Controllers
{
    [Authorize]
    public class CheckoutController : Controller
    {
        const string PromoCode = "FREE";
        private IDocumentSession session = MvcApplication.CurrentSession;
        //
        // GET: /Checkout/AddressAndPayment

        public ActionResult AddressAndPayment()
        {
            return View();
        }

        //
        // POST: /Checkout/AddressAndPayment

        [HttpPost]
        public ActionResult AddressAndPayment(FormCollection values)
        {
            var order = new Order();
            TryUpdateModel(order);

            try
            {
                if (string.Equals(values["PromoCode"], PromoCode, 
                    StringComparison.OrdinalIgnoreCase) == false)
                {
                    return View(order);
                }

                order.Username = User.Identity.Name;
                order.OrderDate = DateTime.Now;

                //Process the order
                var cart = ShoppingCartFinder.FindShoppingCart();
                cart.CreateOrder(order);

                session.Store(order);
                session.SaveChanges();

                return RedirectToAction("Complete", 
                                        new { id = order.Id });
            }
            catch
            {
                //Invalid - redisplay with errors
                return View(order);
            }
        }

        //
        // GET: /Checkout/Complete

        public ActionResult Complete(string id)
        {
            var order = session.Load<Order>(id);
            if (order == null || order.Username != User.Identity.Name)
                return View("Error");
            return View((object)id);
        }
    }
}