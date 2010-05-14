using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Web.Mvc;

namespace MvcMusicStore.Models
{
    [MetadataType(typeof(OrderMetadata))]
    public partial class Order
    {
        // Validation rules for the Order class

        [Bind(Exclude = "OrderId")]
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
            public object OrderId { get; set; }

            [ScaffoldColumn(false)]
            public object OrderDate { get; set; }

            [ScaffoldColumn(false)]
            public object Username { get; set; }

            [ScaffoldColumn(false)]
            public object Total { get; set; }
        }
    }
}