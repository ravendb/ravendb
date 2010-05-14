<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<int>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
    Checkout Complete
</asp:Content>
<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h2>
        Checkout Complete
    </h2>

    <p>
        Thanks for your order! Your order number is:
        <%: Model %>
    </p>

    <p>
        How about shopping for some more music in our 
        <%: Html.ActionLink("store", "Index", "Home") %>?
    </p>

</asp:Content>
