<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.ViewModels.StoreManagerViewModel>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
    Create Album
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h2>Create</h2>

    <% Html.EnableClientValidation(); %>

    <% using (Html.BeginForm()) {%>

    <fieldset>
        <legend>Create Album</legend>
        <%: Html.EditorFor(model => model.Album, new { Artists = Model.Artists, Genres = Model.Genres })%>
        <p>
            <input type="submit" value="Save" />
        </p>
    </fieldset>

    <% } %>

    <div>
        <%:Html.ActionLink("Back to Albums", "Index") %>
    </div>


</asp:Content>
