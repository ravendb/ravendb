using System.Collections.Generic;
using MvcMusicStore.Models;

namespace MvcMusicStore.ViewModels
{
    public class StoreManagerViewModel
    {
        public Album2 Album { get; set; }
        public List<Artist> Artists { get; set; }
        public List<Genre2> Genres { get; set; }
    }
}