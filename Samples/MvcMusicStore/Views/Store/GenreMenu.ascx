<%@ Control Language="C#" Inherits="System.Web.Mvc.ViewUserControl<IEnumerable<MvcMusicStore.Models.Genre>>" %>

<ul>
    <% foreach (var genre in Model) { %>
    <li>
        <%: Html.ActionLink(genre.Name, "Browse", "Store", new { id = genre.Id }, null)%>
    </li>
    <% } %>
</ul>
