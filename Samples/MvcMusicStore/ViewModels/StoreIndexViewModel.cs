using System.Collections.Generic;
using MvcMusicStore.Models;

namespace MvcMusicStore.ViewModels
{
    public class StoreIndexViewModel
    {
        public int NumberOfGenres { get; set; }
        public IList<Genre> Genres { get; set; }
    }
}
