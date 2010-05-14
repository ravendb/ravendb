<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
	Album Deleted
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h2>Album Deleted</h2>

    <p>Your album was successfully deleted.</p>

    <p>
        <%: Html.ActionLink("Click here", "Index") %>
        to return to the album list.
     </p>

</asp:Content>
