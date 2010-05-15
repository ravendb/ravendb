using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Web.Mvc;

namespace MvcMusicStore.Models
{
    [MetadataType(typeof(AlbumMetaData))]
    public partial class Album2
    {
        // Validation rules for the Album class

        #region Nested type: AlbumMetaData

        [Bind(Exclude = "AlbumId")]
        public class AlbumMetaData
        {
            [ScaffoldColumn(false)]
            public object AlbumId { get; set; }

            [DisplayName("Genre")]
            public object GenreId { get; set; }

            [DisplayName("Artist")]
            public object ArtistId { get; set; }

            [Required(ErrorMessage = "An Album Title is required")]
            [StringLength(160)]
            public object Title { get; set; }

            [DisplayName("Album Art URL")]
            [StringLength(1024)]
            public object AlbumArtUrl { get; set; }

            [Required(ErrorMessage = "Price is required")]
            [Range(0.01, 100.00, ErrorMessage = "Price must be between 0.01 and 100.00")]
            public object Price { get; set; }
        }

        #endregion
    }

    public class Album
    {
        public string Id { get; set; }
        public string AlbumArtUrl { get; set; }
        public AlbumArtist Arist { get; set; }
        public AlbumGenre Genre { get; set; }
        public decimal Price { get; set; }
        public string Title { get; set; }
        public int CountSold { get; set; }

        public class AlbumArtist
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class AlbumGenre
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}