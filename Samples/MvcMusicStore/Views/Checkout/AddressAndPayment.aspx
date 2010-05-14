<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<MvcMusicStore.Models.Order>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
    Shipping Address
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <script src="/Scripts/MicrosoftAjax.js" type="text/javascript"></script>
    <script src="/Scripts/MicrosoftMvcAjax.js" type="text/javascript"></script>
    <script src="/Scripts/MicrosoftMvcValidation.js" type="text/javascript"></script>
    <script src="/Scripts/jquery-1.4.1.min.js" type="text/javascript"></script>

    <% Html.EnableClientValidation(); %>
    <% using (Html.BeginForm()) {%>

    <fieldset>
        <legend>Shipping Information</legend>
        <%: Html.EditorForModel() %>
    </fieldset>

    <fieldset>

        <legend>Payment</legend>

        <p>
            We're running a promotion: all music is free with the promo code "FREE"
        </p>

        <div class="editor-label">
            <%: Html.Label("Promo Code") %>
        </div>

        <div class="editor-field">
            <%: Html.TextBox("PromoCode") %>
        </div>

    </fieldset>

    <input type="submit" value="Submit Order" />
    <% } %>
</asp:Content>
