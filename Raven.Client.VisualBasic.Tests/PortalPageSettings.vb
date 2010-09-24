Public Class PortalPageSettings
    Private _Zones As IDictionary(Of String, PortalZone)

    Public Property Zones() As IDictionary(Of String, PortalZone)
        Get
            Return _Zones
        End Get
        Set(ByVal value As IDictionary(Of String, PortalZone))
            _Zones = value
        End Set
    End Property
End Class