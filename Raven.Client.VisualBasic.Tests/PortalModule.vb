

Public Class PortalModule

    Private _ModuleId As String
    Public Property ModuleId() As String
        Get
            Return _ModuleId
        End Get
        Set(ByVal value As String)
            _ModuleId = value
        End Set
    End Property

End Class
