Imports Raven.Client.Client
Imports Raven.Client.Indexes
Imports Raven.Client.Document
Imports Raven.Client.Tests
Imports Xunit

Public Class LinqQueriesUsingVB
    Inherits LocalClientTest

    <Fact()> _
    Public Sub CanUseSelectMany_WithFixedParameter()

        Using store As EmbeddablDocumentStore = NewDocumentStore()

            store.DatabaseCommands.PutIndex("test", New IndexDefinition(Of PortalPageSettings)() With
            { _
                .Map = Function(pages) From page In pages _
                From IdxMod In page.Zones("Left").Modules _
                Select New With {IdxMod.ModuleId}
            }.ToIndexDefinition(store.Conventions))

        End Using

    End Sub


    <Fact()> _
    Public Sub CanUseSelectMany_WithVaraible()

        Using store As EmbeddablDocumentStore = NewDocumentStore()

            store.DatabaseCommands.PutIndex("test", New IndexDefinition(Of PortalPageSettings)() With
            { _
                .Map = Function(pages) From page In pages _
                                         From PaneName In page.Zones.Keys _
                From IdxMod In page.Zones(PaneName).Modules _
                Select New With {IdxMod.ModuleId}
            }.ToIndexDefinition(store.Conventions))

        End Using

    End Sub
    
End Class