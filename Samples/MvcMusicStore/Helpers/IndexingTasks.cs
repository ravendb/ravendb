using System.Linq;
using MvcMusicStore.Models;
using Raven.Client.Indexes;
using Raven.Database.Indexing;

namespace MvcMusicStore.Helpers
{
    public static class IndexingTasks
    {
        public class SoldAlbums_Count : AbstractIndexCreationTask<SoldAlbum>
        {
            public SoldAlbums_Count()
            {
                Map = soldAlbums => from album in soldAlbums
                                    select new { album.Album, Quantity = 1 };
                Reduce = results => from result in results
                                    group result by result.Album
                                        into g
                                        select new
                                                   {
                                                       Album = g.Key,
                                                       Quantity = g.Sum(x => x.Quantity)
                                                   };
            }
        }

        public class Albums_CountSold : AbstractIndexCreationTask<Album>
        {
            public Albums_CountSold()
            {
                Map = albums => from album in albums
                                select new { album.Id, album.CountSold };

                Index(x => x.CountSold, FieldIndexing.NotAnalyzedNoNorms);
            }
        }

        public class Albums_ByGenre : AbstractIndexCreationTask<Album>
        {
            public Albums_ByGenre()
            {
                Map = albums => from album in albums
                                select new { album.Genre.Name };

                Index(x => x.Genre.Name, FieldIndexing.NotAnalyzedNoNorms);
            }
        }
    }
}