<%@ Page Title="" Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.ViewModels.ShoppingCartViewModel>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
    Shopping Cart
</asp:Content>
<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <script src="/Scripts/MicrosoftAjax.js" type="text/javascript"></script>
    <script src="/Scripts/MicrosoftMvcAjax.js" type="text/javascript"></script>
    <script src="/Scripts/jquery-1.4.1.min.js" type="text/javascript"></script>

    <script type="text/javascript">
        function handleUpdate(context) {
            // Load and deserialize the returned JSON data
            var json = context.get_data();
            var data = Sys.Serialization.JavaScriptSerializer.deserialize(json);

            // Update the page elements
            $('#row-' + data.DeleteId).fadeOut('slow');
            $('#cart-status').text('Cart (' + data.CartCount + ')');
            $('#update-message').text(data.Message);
            $('#cart-total').text(data.CartTotal);
        }
    </script>

    <h3>
        <em>Review</em> your cart:
    </h3>
    <p class="button">
        <%: Html.ActionLink("Checkout >>", "AddressAndPayment", "Checkout")%>
    </p>

    <div id="update-message"></div>

    <table>

        <tr>
            <th>Album Name</th>
            <th>Price (each)</th>
            <th>Quantity</th>
            <th></th>
        </tr>

        <% foreach (var item in Model.CartItems) { %>
        <tr id="row-<%: item.RecordId %>">
            <td>
                <%: Html.ActionLink(item.Album.Title, "Details", "Store", new { id = item.AlbumId }, null)%>
            </td>
            <td>
                <%: item.Album.Price %>
            </td>
            <td>
                <%: item.Count %>
            </td>
            <td>
                <%: Ajax.ActionLink("Remove from cart", "RemoveFromCart", new { id = item.RecordId }, new AjaxOptions { OnSuccess = "handleUpdate" })%>
            </td>
        </tr>
        <% } %>

        <tr>
            <td>Total</td>
            <td></td>
            <td></td>
            <td id="cart-total">
                <%: Model.CartTotal %>
            </td>
        </tr>

    </table>

</asp:Content>
