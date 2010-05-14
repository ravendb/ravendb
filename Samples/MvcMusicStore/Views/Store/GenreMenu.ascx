<%@ Control Language="C#" Inherits="System.Web.Mvc.ViewUserControl<IEnumerable<MvcMusicStore.Models.Genre>>" %>

<ul>
    <% foreach (var genre in Model) { %>
    <li>
        <%: Html.ActionLink(genre.Name, "Browse", "Store", new { Genre = genre.Name }, null)%>
    </li>
    <% } %>
</ul>
