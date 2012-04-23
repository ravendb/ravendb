namespace Raven.Client

open System
open Raven.Imports.Newtonsoft.Json
open System.Configuration
open Raven.Client.Document

[<AutoOpen>]
module DocumentStoreExt =
    
    type Raven.Client.Document.DocumentStore with
        
        static member OpenInitializedStore(name) = 
            let store = new DocumentStore (ConnectionStringName = name)
            store.Conventions.CustomizeJsonSerializer <- (fun s -> s.Converters.Add(new UnionTypeConverter()))
            store.Initialize() |> ignore
            store


