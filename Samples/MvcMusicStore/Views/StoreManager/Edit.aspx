<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.ViewModels.StoreManagerViewModel>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
	Edit - <%: Model.Album.Title %>
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h2>Edit Album</h2>

    <% Html.EnableClientValidation(); %>

    <% using (Html.BeginForm()) {%>

    <fieldset>
        <legend>Edit Album</legend>
        <%: Html.EditorFor(model => model.Album, new { Artists = Model.Artists, Genres = Model.Genres}) %>
        <p>
            <input type="submit" value="Save" />
        </p>
    </fieldset>

    <% } %>

    <div>
        <%:Html.ActionLink("Back to Albums", "Index") %>
    </div>

</asp:Content>

