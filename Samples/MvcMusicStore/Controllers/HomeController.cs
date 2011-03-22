//-----------------------------------------------------------------------
// <copyright file="HomeController.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Web.Mvc;
using MvcMusicStore.Models;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Indexing;

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

            return session.Query<Album>()
                .OrderByDescending(x => x.CountSold)
                .Take(count);
        }

        public ActionResult AddCountSoldtoAlbum()
        {
            var session = MvcApplication.CurrentSession;
            int count = 0;
            do
            {
                var albums = session.Query<Album>()
                    .Skip(count)
                    .Take(128)
                    .ToArray();

                if (albums.Length == 0)
                    break;

                foreach (var album in albums)
                {
                    var id = album.Id;

                    var result = session.Query<SoldAlbum>("SoldAlbums")
                        .Where(x => x.Album == id)
                        .SingleOrDefault();

                    album.CountSold = (result == null) ? 0 : result.Quantity;
                }

                count += albums.Length;

                session.SaveChanges();
            } while (true);

            return Content("OK");
        }


#if  STILL_PORTING
        public ActionResult PortToRaven()
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
