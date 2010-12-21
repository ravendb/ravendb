//-----------------------------------------------------------------------
// <copyright file="StoreManagerViewModel.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using MvcMusicStore.Models;

namespace MvcMusicStore.ViewModels
{
    public class StoreManagerViewModel
    {
        public Album Album { get; set; }
        public List<Album.AlbumArtist> Artists { get; set; }
        public List<Genre> Genres { get; set; }
    }
}
