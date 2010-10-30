Public Class PortalPageSettings
    Private _Zones As IDictionary(Of String, PortalZone)

    Private _name As String
    Public Property Name() As String
        Get
            Return _name
        End Get
        Set(ByVal value As String)
            _name = value
        End Set
    End Property



    Public Property Zones() As IDictionary(Of String, PortalZone)
        Get
            Return _Zones
        End Get
        Set(ByVal value As IDictionary(Of String, PortalZone))
            _Zones = value
        End Set
    End Property
End Class