using System.Collections.Generic;
using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using Raven.Client.Document;

namespace MvcMusicStore.Controllers
{
    public class HomeController : Controller
    {
        //
        // GET: /Home/

        MusicStoreEntities storeDB = new MusicStoreEntities();

        public ActionResult Index()
        {
            // Get most popular albums
            var albums = GetTopSellingAlbums(5);

            return View(albums);
        }


        public ActionResult Port()
        {
using (var documentStore = new DocumentStore
{
    Url = "http://localhost:8080",
    Conventions =
        {
            FindTypeTagName = type =>
            {
                if (type.GetProperty("Genre") == null)
                    return "Genres";
                return "Albums";
            }
        }
})
{
    documentStore.Initialise();
    using (var session = documentStore.OpenSession())
    {
        session.OnEntityConverted += (entity, document, metadata) => metadata.Remove("Raven-Clr-Type");
        foreach (var album in storeDB.Albums.Include("Artist").Include("Genre"))
        {
            session.Store(new
            {
                Id = "albums/" + album.AlbumId,
                album.AlbumArtUrl,
                Arist = new { album.Artist.Name, Id = "artists/" + album.Artist.ArtistId },
                Genre = new { album.Genre.Name, Id = "genres/" + album.Genre.GenreId },
                album.Price,
                album.Title,
            });
        }
        foreach (var genre in storeDB.Genres)
        {
            session.Store(new
            {
                genre.Description,
                genre.Name,
                Id = "genres/" + genre.GenreId
            });
        }
        session.SaveChanges();
    }
}
            return Content("OK");
        }

        private List<Album> GetTopSellingAlbums(int count)
        {
            // Group the order details by album and return
            // the albums with the highest count

            return storeDB.Albums
                .OrderByDescending(a => a.OrderDetails.Count())
                .Take(count)
                .ToList();
        }
    }
}
