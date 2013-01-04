Imports Raven.Client.Linq.Indexing
Imports Raven.Client.Embedded
Imports Raven.Client.Indexes
Imports Raven.Tests
Imports Xunit
Imports Raven.Client.Linq

Public Class LinqQueriesUsingVB
	Inherits RavenTest

	<Fact()> _
	Public Sub CanUseWhereEntityIs()
		Using store As EmbeddableDocumentStore = NewDocumentStore()
			Dim x = New Data_NewIndex()
			x.Execute(store.DatabaseCommands, store.Conventions)
		End Using
	End Sub


	<Fact()> _
	Public Sub CanUseSelectMany_WithFixedParameter()

		Using store As EmbeddableDocumentStore = NewDocumentStore()

			store.DatabaseCommands.PutIndex("test", New IndexDefinitionBuilder(Of PortalPageSettings)() With
			{ _
			 .Map = Function(pages) From page In pages _
			 From IdxMod In page.Zones("Left").Modules _
			 Select New With {IdxMod.ModuleId}
			}.ToIndexDefinition(store.Conventions))

		End Using

	End Sub

	<Fact()> _
	Public Sub CanQueryUsingStringProperty()

		Using store = NewDocumentStore()

			Using session = store.OpenSession()

				session.Query(Of PortalPageSettings)().Where(Function(x) x.Name = "ayende").FirstOrDefault()

				Dim query = session.Query(Of PortalPageSettings)().Where(Function(x) x.Name = "ayende").ToString()

				Assert.Equal("Name:ayende", query)

				Dim user = "rahien"

				query = session.Query(Of PortalPageSettings)().Where(Function(x) x.Name = user).ToString()

				user = "oren"

				Assert.Equal("Name:rahien", query)

				query = session.Query(Of PortalPageSettings)().Where(Function(x) x.Name = user).ToString()

				Assert.Equal("Name:oren", query)

			End Using

		End Using

	End Sub

	<Fact()> _
	Public Sub CanUseWithEntityIsExtensionMethod()

		Using store As EmbeddableDocumentStore = NewDocumentStore()

			Dim index As New IndexDefinitionBuilder(Of Object)() With
			{ _
			 .Map = Function(pages) From page In pages.WhereEntityIs(Of PortalPageSettings)("Ayende", "Rahien") _
			 From IdxMod In page.Zones("Left").Modules _
			 Select New With {IdxMod.ModuleId}
			}

			store.DatabaseCommands.PutIndex("test", index.ToIndexDefinition(store.Conventions))

		End Using

	End Sub

	<Fact()> _
	Public Sub EntityIsExtensionMethodWillBeTranslatedProperly()

		Using store As EmbeddableDocumentStore = NewDocumentStore()

			Dim index As New IndexDefinitionBuilder(Of Object)() With
			{ _
			 .Map = Function(pages) From page In pages.WhereEntityIs(Of PortalPageSettings)("Ayende", "Rahien") _
			 From IdxMod In page.Zones("Left").Modules _
			 Select New With {IdxMod.ModuleId}
			}

			store.DatabaseCommands.PutIndex("test", index)
		End Using

	End Sub

	<Fact()> _
	Public Sub CanUseSelectMany_WithVaraible()

		Using store As EmbeddableDocumentStore = NewDocumentStore()

			store.DatabaseCommands.PutIndex("test", New IndexDefinitionBuilder(Of PortalPageSettings)() With
			{ _
			 .Map = Function(pages) From page In pages _
					From PaneName In page.Zones.Keys _
			 From IdxMod In page.Zones(PaneName).Modules _
			 Select New With {IdxMod.ModuleId}
			}.ToIndexDefinition(store.Conventions))

		End Using

	End Sub

End Class