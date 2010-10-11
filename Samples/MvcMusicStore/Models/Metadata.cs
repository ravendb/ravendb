using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Web.Mvc;

namespace MvcMusicStore.Models
{
    [MetadataType(typeof(OrderMetadata))]
    public partial class Order
    {
        [Bind(Exclude = "Id")]
        public class OrderMetadata
        {
            [Required(ErrorMessage = "First Name is required")]
            [DisplayName("First Name")]
            [StringLength(160)]
            public object FirstName { get; set; }

            [Required(ErrorMessage = "Last Name is required")]
            [DisplayName("Last Name")]
            [StringLength(160)]
            public object LastName { get; set; }

            [Required(ErrorMessage = "Address is required")]
            [StringLength(70)]
            public object Address { get; set; }

            [Required(ErrorMessage = "City is required")]
            [StringLength(40)]
            public object City { get; set; }

            [Required(ErrorMessage = "State is required")]
            [StringLength(40)]
            public object State { get; set; }

            [Required(ErrorMessage = "Postal Code is required")]
            [DisplayName("Postal Code")]
            [StringLength(10)]
            public object PostalCode { get; set; }

            [Required(ErrorMessage = "Country is required")]
            [StringLength(40)]
            public object Country { get; set; }

            [Required(ErrorMessage = "Phone is required")]
            [StringLength(24)]
            public object Phone { get; set; }

            [Required(ErrorMessage = "Email Address is required")]
            [DisplayName("Email Address")]
            [RegularExpression(@"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}", ErrorMessage = "Email is is not valid.")]
            [DataType(DataType.EmailAddress)]
            public object Email { get; set; }

            [ScaffoldColumn(false)]
            public object Id { get; set; }

            [ScaffoldColumn(false)]
            public object OrderDate { get; set; }

            [ScaffoldColumn(false)]
            public object Username { get; set; }

            [ScaffoldColumn(false)]
            public object Total { get; set; }
        }
    }

    [MetadataType(typeof(AlbumMetaData))]
    public partial class Album
    {
        // Validation rules for the Album class

        [Bind(Exclude = "Id")]
        public class AlbumMetaData
        {
            [ScaffoldColumn(false)]
            public object Id { get; set; }

            [DisplayName("Genre")]
            public object Genre { get; set; }

            [DisplayName("Artist")]
            public object Artist { get; set; }

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
    }
}
