Imports System
Imports Raven.Client.Linq.Indexing
Imports Raven.Abstractions.Indexing
Imports Raven.Client.Linq
Imports Raven.Client.Indexes
Imports Raven.Database.Indexing

''' <summary>
''' Creates an index on objects private Ids
''' </summary>
''' <remarks></remarks>
Public Class Data_NewIndex
	Inherits AbstractIndexCreationTask

	Public Overrides Function CreateIndexDefinition() As IndexDefinition
		Return (New IndexDefinitionBuilder(Of TestClass)() With { _
		  .Map = Function(items) From item In items.WhereEntityIs(Of TestClass)("TestClass", "TestClass2") _
				  Select New With {item.ResourceKey}
		}.ToIndexDefinition(Conventions))
	End Function
End Class