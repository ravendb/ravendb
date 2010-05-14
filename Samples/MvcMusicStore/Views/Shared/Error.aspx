<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage" %>

<asp:Content ID="Content1" ContentPlaceHolderID="TitleContent" runat="server">
	Error
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">

    <h2>Error</h2>

    <p>
        We're sorry, we've hit an unexpected error. 
        <a href="javascript:history.go(-1)">Click here</a> 
        if you'd like to go back and try that again.
    </p>

</asp:Content>
