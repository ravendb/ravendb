Public Class PortalZone

    Private _modules As IList(Of PortalModule)
    Public Property Modules() As IList(Of PortalModule)
        Get
            Return _modules
        End Get
        Set(ByVal value As IList(Of PortalModule))
            _modules = value
        End Set
    End Property

End Class