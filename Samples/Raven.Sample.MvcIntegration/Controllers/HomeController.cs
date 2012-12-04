using System.Collections.Generic;
using System.Web.Mvc;
using Raven.Sample.MvcIntegration.Models;
using System.Linq;

namespace Raven.Sample.MvcIntegration.Controllers
{
	public class HomeController : Controller
	{
		public ActionResult Index()
		{
			using (var session = WebApiApplication.Store.OpenSession())
			{
				session.Store(new TodoItem { Text = "Getting Started!" });
				session.SaveChanges();
			}

			using (var session = WebApiApplication.Store.OpenSession())
			{
				var todoItems = session.Query<TodoItem>().ToList();
			}
			return View();
		}

		public ActionResult NoRequests()
		{
			return View("Index");
		}

		public JsonResult SingleSessionGet()
		{
			List<TodoItem> items;
			using (var session = WebApiApplication.Store.OpenSession())
			{
				items = session.Query<TodoItem>().ToList();
			}
			return Json(new { items }, JsonRequestBehavior.AllowGet);
		}

		public JsonResult MultiSessionGet()
		{
			var items = new List<TodoItem>();
			using (var session = WebApiApplication.Store.OpenSession())
			{
				items.AddRange(session.Query<TodoItem>().ToList());
			}
			using (var session = WebApiApplication.Store.OpenSession())
			{
				items.AddRange(session.Query<TodoItem>().ToList());
			}
			
			return Json(new {items}, JsonRequestBehavior.AllowGet);
		}
	}
}