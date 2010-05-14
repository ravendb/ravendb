using System.Collections.Generic;

namespace MvcMusicStore.ViewModels
{
    public class StoreIndexViewModel
    {
        public int NumberOfGenres { get; set; }
        public List<string> Genres { get; set; }
    }
}