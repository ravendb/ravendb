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
				session.Store(new TodoItem {Text = "Getting Started!"});
				session.SaveChanges();
			}

			using (var session = WebApiApplication.Store.OpenSession())
			{
				var todoItems = session.Query<TodoItem>().ToList();
			}
			return View();
		}
	}
}