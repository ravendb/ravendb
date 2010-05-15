using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using MvcMusicStore.ViewModels;
using Raven.Client;

namespace MvcMusicStore.Controllers
{
    public class StoreController : Controller
    {
        //
        // GET: /Store/

        public ActionResult Index()
        {
            var session = MvcApplication.CurrentSession;
            // Retrieve list of Genres from database
            var genres = session.Query<Genre>()
                .ToArray();

            // Set up our ViewModel
            var viewModel = new StoreIndexViewModel()
            {
                Genres = genres,
                NumberOfGenres = genres.Length
            };

            // Return the view
            return View(viewModel);
        }

        //
        // GET: /Store/Browse?Genre=Disco

        public ActionResult Browse(string genre)
        {
            // Retrieve Genre from database
            var session = MvcApplication.CurrentSession;

            var genreModel = session.Load<Genre>(genre);
            var albums = session.Query<Album>("AlbumsByGenre")
                .Where("Genre:" + genre)
                .ToArray();
            var viewModel = new StoreBrowseViewModel()
            {
                Genre = genreModel,
                Albums = albums
            };

            return View(viewModel);
        }

        //
        // GET: /Store/Details/5

        public ActionResult Details(string album)
        {
            var session = MvcApplication.CurrentSession;
            var albumModel = session.Load<Album>(album);
            return View(albumModel);
        }

        //
        // GET: /Store/GenreMenu

        [ChildActionOnly]
        public ActionResult GenreMenu()
        {
            var session = MvcApplication.CurrentSession;
            return View(session.Query<Genre>()
                .ToArray());
        }
    }
}