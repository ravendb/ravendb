using System.Collections.Generic;
using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using Raven.Client.Document;
using Raven.Client;

namespace MvcMusicStore.Controllers
{
    public class HomeController : Controller
    {
        //
        // GET: /Home/

        public ActionResult Index()
        {
            // Get most popular albums
            var albums = GetTopSellingAlbums(5);

            return View(albums);
        }

        private IEnumerable<Album> GetTopSellingAlbums(int count)
        {
            var session = MvcApplication.CurrentSession;

            // Get count most sold albums ids
            var topSoldAlbumIds = session.Query<SoldAlbum>("SoldAlbums")
                .OrderBy("-Quantity")
                .Take(count)
                .Select(x=>x.Album)
                .ToArray();

            // get the actual album documents
            var topSoldAlbums = session.Load<Album>(topSoldAlbumIds);
            // if we don't have enough sold albums
            if(topSoldAlbums.Length < count)
            {
                // top it off from the unsold albums
                var justRegularAlbums = session.Query<Album>()
                    .Take(count);
                topSoldAlbums = topSoldAlbums.Concat(justRegularAlbums)
                    .Distinct()
                    .Take(count)
                    .ToArray();
            }
            return topSoldAlbums;
        }

        public class SoldAlbum
        {
            public string Album { get; set; }
            public int Quantity{ get; set; }
        }

#if  !STILL_PORTING
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
                    foreach (var album in new MusicStoreEntities().Albums.Include("Artist").Include("Genre"))
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
                    foreach (var genre in new MusicStoreEntities().Genres)
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
#endif
    }
}
