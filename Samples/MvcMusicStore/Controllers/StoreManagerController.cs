using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using MvcMusicStore.ViewModels;
using Raven.Client;

namespace MvcMusicStore.Controllers
{
    [HandleError]
    //[Authorize(Roles = "Administrator")]
    public class StoreManagerController : Controller
    {
        private IDocumentSession session = MvcApplication.CurrentSession;

        //
        // GET: /StoreManager/

        public ActionResult Index()
        {
            var albums = session.Advanced.LuceneQuery<Album>().ToArray();

            return View(albums);
        }

        //
        // POST: /StoreManager/Create

        [HttpPost]
        public ActionResult Create(Album album)
        {
            //Save Album
            session.Store(album);
            session.SaveChanges();

            return Redirect("/");
        }

        //
        // GET: /StoreManager/Edit/5

        public ActionResult Edit(string id)
        {
            var viewModel = new StoreManagerViewModel
            {
                Album = session.Load<Album>(id),
                Genres = session.Advanced.LuceneQuery<Genre>().ToList(),
                Artists = session.Advanced.LuceneQuery<Album.AlbumArtist>("Artists").ToList()
            };

            return View(viewModel);
        }


        // 
        // GET: /StoreManager/Create

        public ActionResult Create()
        {
            var viewModel = new StoreManagerViewModel
            {
                Album = new Album(),
                Genres = session.Advanced.LuceneQuery<Genre>().ToList(),
                Artists = session.Advanced.LuceneQuery<Album.AlbumArtist>("Artists").ToList()
            };

            return View(viewModel);
        }

        //
        // POST: /StoreManager/Edit/5

        [HttpPost]
        public ActionResult Edit(string id, FormCollection formValues)
        {
            var albumModel = session.Load<Album>(id);
            //Save Album

            UpdateModel(albumModel, "Album");
            session.SaveChanges();

            return RedirectToAction("Index");
        }

        //
        // GET: /StoreManager/Delete/5

        public ActionResult Delete(string id)
        {
            var albumModel = session.Load<Album>(id);

            return View(albumModel);
        }

        //
        // POST: /StoreManager/Delete/5

        [HttpPost]
        public ActionResult Delete(string id, string confirmButton)
        {
            session.Delete(session.Load<Album>(id));
            session.SaveChanges();


            return View("Deleted");
        }
    }
}
