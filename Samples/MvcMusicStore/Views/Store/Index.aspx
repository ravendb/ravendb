<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.ViewModels.StoreIndexViewModel>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
	Browse Genres
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h3>Browse Genres</h3>

    <p>Select from <%: Model.NumberOfGenres %> genres:</p>

    <ul>
        <% foreach (var genre in Model.Genres) { %>
           <li>
            <%: Html.ActionLink(genre.Name, "Browse", "Store", new { genre = genre.Id }, null)%>
           </li>
        <% } %>
    </ul>

</asp:Content>
