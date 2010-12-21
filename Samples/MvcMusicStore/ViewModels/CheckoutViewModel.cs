//-----------------------------------------------------------------------
// <copyright file="CheckoutViewModel.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using MvcMusicStore.Models;

namespace MvcMusicStore.ViewModels
{
    public class CheckoutViewModel
    {
        public List<Album> Albums { get; set; }
        public Order Oustomer { get; set; }
    }
}
