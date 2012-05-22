namespace Raven.Tests

open Raven.Client
open Raven.Client.Indexes

type TotalsReduceResult = {
    Year : int
    Month : int
    Day : int
    Total : float
}

(*
    This version uses calls to the F# Seq module, and is compatible with F# 2.0

    The unit test fails with this:

    Test Name:	Should be able to create an index
    Test FullName:	Raven.Tests.Given an Initailised Document store execute using computation expression.Should be able to create an index
    Test Source:	D:\Projects\ravendb\Raven.Tests.FSharp\RavenTests.fs : line 268
    Test Outcome:	Failed
    Test Duration:	0:00:02.398

    Result Message:	System.InvalidOperationException : Canot understand how to parse the query
    Result StackTrace:	
    at Raven.Client.Indexes.IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode[TQueryRoot,TReduceResult](LambdaExpression expr, DocumentConvention convention, String querySource, Boolean translateIdentityProperty) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexDefinitionHelper.cs:line 67
       at Raven.Client.Indexes.IndexDefinitionBuilder`2.ToIndexDefinition(DocumentConvention convention, Boolean validateMap) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexDefinitionBuilder.cs:line 98
       at Raven.Client.Indexes.AbstractIndexCreationTask`2.CreateIndexDefinition() in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\AbstractIndexCreationTask.cs:line 173
       at Raven.Client.Indexes.AbstractIndexCreationTask.Execute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\AbstractIndexCreationTask.cs:line 117
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 46
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 57
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 34
       at Raven.Tests.Given an Initailised Document store execute using computation expression.Should be able to create an index() in D:\Projects\ravendb\Raven.Tests.FSharp\RavenTests.fs:line 269

*)


type OrderTotalsByDay() as this =
    inherit AbstractIndexCreationTask<Order, TotalsReduceResult>()

    let map = 
        <@ Seq.map (fun (o:Order) -> 
                        { Year = o.Date.Year; 
                          Month = o.Date.Month; 
                          Day = o.Date.Day;
                          Total = o.Items |> Seq.sumBy (fun i -> i.Price) }) @>

    let reduce =
        <@ Seq.groupBy (fun (result:TotalsReduceResult) -> result.Year, result.Month, result.Day)
           >> Seq.map (fun ((year, month, day), results) ->
                           { Year = year;
                             Month = month;
                             Day = day;
                             Total = results |> Seq.sumBy (fun r -> r.Total) }) @>
                        
    do
        this.Map <- Linq.toIndexExpression map
        this.Reduce <- Linq.toIndexExpression reduce


(*
    The following version uses the F# 3.0 built-in LINQ support, in the hopes it provides
    an easier-to-parse expression tree.

    To get this to compile, you must use Visual Studio 11 (or the F# 3.0 compiler),
    and you must:
        - change all the F# projects to reference FSharp.Core, 4.3.0.0 instead of 4.0.0.0.
        - set the F# projects to target .NET 4.5
        - reference a version of FSharp.Powerpack.Linq.dll that has been compiled for .NET 4.5
          and FSharp.Core 4.3.0.0, or simply include Linq.fsi and Linq.fs from the powerpack 
          into the Raven.Client.Lightweight.FSharp project

    The unit test fails with this:

    Test Name:	Should be able to create an index
    Test FullName:	Raven.Tests.Given an Initailised Document store execute using computation expression.Should be able to create an index
    Test Source:	D:\Projects\ravendb\Raven.Tests.FSharp\RavenTests.fs : line 268
    Test Outcome:	Failed
    Test Duration:	0:00:02.266

    Result Message:	
    System.ComponentModel.Composition.CompositionException : The composition produced a single composition error. The root cause is provided below. Review the CompositionException.Errors property for more detailed information.

    1) Could not convert the following F# Quotation to a LINQ Expression Tree
    --------
    Quote (Call (Some (builder@), Select,
                 [Call (Some (builder@), For,
                        [Call (Some (builder@), Source, [orders]),
                         Lambda (_arg,
                                 Let (o, _arg, Call (Some (builder@), Yield, [o])))]),
                  Lambda (o,
                          NewRecord (TotalsReduceResult,
                                     PropertyGet (Some (PropertyGet (Some (o), Date,
                                                                     [])), Year, []),
                                     PropertyGet (Some (PropertyGet (Some (o), Date,
                                                                     [])), Month, []),
                                     PropertyGet (Some (PropertyGet (Some (o), Date,
                                                                     [])), Day, []),
                                     Call (None, op_PipeRight,
                                           [PropertyGet (Some (o), Items, []),
                                            Let (projection,
                                                 Lambda (i,
                                                         PropertyGet (Some (i),
                                                                      Price, [])),
                                                 Lambda (source,
                                                         Call (None, SumBy,
                                                               [projection,
                                                                Coerce (source,
                                                                        IEnumerable`1)])))])))]))
    -------------


    Resulting in: An exception occurred while trying to create an instance of type 'Raven.Tests.OrderTotalsByDay'.

    Resulting in: Cannot activate part 'Raven.Tests.OrderTotalsByDay'.
    Element: Raven.Tests.OrderTotalsByDay -->  Raven.Tests.OrderTotalsByDay -->  AssemblyCatalog (Assembly="Raven.Tests.FSharp, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null")

    Resulting in: Cannot get export 'Raven.Client.Indexes.AbstractIndexCreationTask (ContractName="Raven.Client.Indexes.AbstractIndexCreationTask")' from part 'Raven.Tests.OrderTotalsByDay'.
    Element: Raven.Client.Indexes.AbstractIndexCreationTask (ContractName="Raven.Client.Indexes.AbstractIndexCreationTask") -->  Raven.Tests.OrderTotalsByDay -->  AssemblyCatalog (Assembly="Raven.Tests.FSharp, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null")
    Result StackTrace:	
    at System.ComponentModel.Composition.Hosting.CompositionServices.GetExportedValueFromComposedPart(ImportEngine engine, ComposablePart part, ExportDefinition definition)
       at System.ComponentModel.Composition.Hosting.CatalogExportProvider.GetExportedValue(CatalogPart part, ExportDefinition export, Boolean isSharedPart)
       at System.ComponentModel.Composition.Hosting.CatalogExportProvider.CatalogExport.GetExportedValueCore()
       at System.ComponentModel.Composition.Primitives.Export.get_Value()
       at System.ComponentModel.Composition.ExportServices.GetCastedExportedValue[T](Export export)
       at System.ComponentModel.Composition.Hosting.ExportProvider.GetExportedValuesCore[T](String contractName)
       at System.ComponentModel.Composition.Hosting.ExportProvider.GetExportedValues[T](String contractName)
       at System.ComponentModel.Composition.Hosting.ExportProvider.GetExportedValues[T]()
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDatabaseCommands databaseCommands, DocumentConvention conventions) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 43
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(ExportProvider catalogToGetnIndexingTasksFrom, IDocumentStore documentStore) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 57
       at Raven.Client.Indexes.IndexCreation.CreateIndexes(Assembly assemblyToScanForIndexingTasks, IDocumentStore documentStore) in d:\Projects\ravendb\Raven.Client.Lightweight\Indexes\IndexCreation.cs:line 34
       at Raven.Tests.Given an Initailised Document store execute using computation expression.Should be able to create an index() in D:\Projects\ravendb\Raven.Tests.FSharp\RavenTests.fs:line 269


*)

//type OrderTotalsByDay() as this =
//    inherit AbstractIndexCreationTask<Order, TotalsReduceResult>()
//
//    let map = 
//        <@ fun (orders:seq<Order>) ->
//            query {
//                for o in orders do
//                select ({ Year = o.Date.Year; 
//                          Month = o.Date.Month; 
//                          Day = o.Date.Day;
//                          Total = o.Items |> Seq.sumBy (fun i -> i.Price) })
//            }
//        @>
//
//    let reduce =
//        <@ fun (results:seq<TotalsReduceResult>) ->
//            query {
//                for r in results do
//                groupValBy r (r.Year, r.Month, r.Day) into g
//                select (let y,m,d = g.Key
//                        { Year = y; Month = m; Day = d; Total = g |> Seq.sumBy (fun r -> r.Total) })
//            }
//        @>
//                        
//    do
//        this.Map <- Linq.toIndexExpression map
//        this.Reduce <- Linq.toIndexExpression reduce