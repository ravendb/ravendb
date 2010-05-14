<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.ViewModels.StoreBrowseViewModel>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
    Browse Albums
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <div class="genre">
        <p><strong><%: Model.Genre.Name %>:</strong> <%: Model.Genre.Description %></p>

        <ul id="album-list">
            <% foreach (var album in Model.Albums) { %>

            <li>
                <a href="<%: Url.Action("Details", new { id = album.AlbumId }) %>">
                    <img alt="<%: album.Title %>" src="<%: album.AlbumArtUrl %>" />
                    <span><%: album.Title %></span>
                </a>
            </li>

            <% } %>
        </ul>

    </div>

</asp:Content>
