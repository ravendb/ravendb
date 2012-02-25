namespace Raven.Client

open System
open System.Linq
open System.Linq.Expressions
open System.ComponentModel.Composition

open Raven.Client
open Raven.Client.Linq
open Raven.Client.Document
open Raven.Abstractions.Extensions
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Linq
open Newtonsoft.Json

 [<AutoOpen>]
 module Operators = 
        
        let query (f : (IRavenQueryable<'a> -> #IQueryable<'b>)) (a : IDocumentSession) = 
            f(a.Query<'a>()).AsEnumerable()
        
        let luceneQuery<'a, 'b> (f : (IDocumentQuery<'a> -> IDocumentQuery<'b>)) (a : IDocumentSession) = 
            f(a.Advanced.LuceneQuery<'a>()).AsEnumerable()

        let queryAllBy f (a : IDocumentSession) : seq<'a> =  
            Seq.unfold (fun (page, results, stats : RavenQueryStatistics) -> 
                            if stats = null 
                            then
                                let s = ref Unchecked.defaultof<RavenQueryStatistics>
                                let r = query (fun q -> f(q.Statistics(s))) a |> Seq.toList
                                let count = r.Length
                                Some <| (r, (page + 1, (results + count), !s))
                            else if stats.TotalResults > results
                            then 
                                let start = (page + stats.SkippedResults) * 128
                                let r = query (fun q -> f(q).Skip(start).Take(128)) a |> Seq.toList
                                let count = r.Length
                                Some <| (r, (page + 1, (results + count), stats))
                            else None
                        ) (0, 0, null)
            |> Seq.concat

        let queryAll<'a> (a : IDocumentSession) : seq<'a> = 
            queryAllBy id a

        let where (p : Expr<'a -> bool>) (a : IRavenQueryable<'a>) =
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,bool>>(a, b |> Array.ofSeq)) p
            a.Where(expr)

        let orderBy descending (p : Expr<'a -> 'b>) (a : IRavenQueryable<'a>) =
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,'b>>(a, b |> Array.ofSeq)) p
            if descending 
            then a.OrderByDescending(expr)
            else a.OrderBy(expr)

        let ``include`` (p : Expr<'a -> 'b>) (loader : ILoaderWithInclude<'a> -> 'c) (a : IDocumentSession) = 
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,obj>>(a, b |> Array.ofSeq)) p
            loader(a.Include<'a>(expr))
        
        let putAttachment documentId etag data metaData (a : IDocumentSession) = 
            a.Advanced.DatabaseCommands.PutAttachment(documentId, etag, data, Raven.Json.Linq.RavenJObject.FromObject(metaData))

        let createAttachment documentId data metaData (a : IDocumentSession) =
            putAttachment documentId (Nullable()) data metaData a

        let getAttachmentAsStream<'a> documentId (stream : IO.Stream) (a : IDocumentSession) =
            let attachment = a.Advanced.DatabaseCommands.GetAttachment(documentId)
            let attachmentBody = attachment.Data.Invoke().CopyTo(stream)
            (attachment.Metadata.JsonDeserialization<'a>(), attachment.Etag)

        let getAttachmentAsBytes<'a> documentId (a : IDocumentSession) =
            let attachment = a.Advanced.DatabaseCommands.GetAttachment(documentId)
            let attachmentBody = attachment.Data.Invoke().ReadData()
            (attachment.Metadata.JsonDeserialization<'a>(), attachment.Etag, attachmentBody)
        
        let load<'a> (id : seq<string>) (a : IDocumentSession) = 
            a.Load(id).Cast<'a>()

        let store input  = 
            (fun (s : IDocumentSession) -> s.Store(input); input)
        
        let commit (s : IDocumentSession) = 
            s.SaveChanges()

        let storeImmediate input =
            (fun (s : IDocumentSession) -> s.Store(input); s.SaveChanges(); input)

        let delete (input : string) = 
            (fun (session : IDocumentSession) ->  
                        session.Advanced.DatabaseCommands.Delete(input, Nullable()))

        let deleteImmediate (input : string) = 
            (fun (session : IDocumentSession) ->  
                        session.Advanced.DatabaseCommands.Delete(input, Nullable())
                        session.SaveChanges())

        let deleteMany (input : seq<'a>) =
            (fun (session : IDocumentSession) ->  
                                let xs = input |> Seq.toList
                                for i in 0 .. (xs.Length - 1) do
                                    session.Store(xs.[i])
                                    session.Delete(xs.[i])
                                    if i = 29 then session.SaveChanges()
                                session.SaveChanges())

        let storeMany input = 
            let storeMany' input (session : IDocumentSession) = 
                let xs = input |> Seq.toList
                for i in 0 .. (xs.Length - 1) do
                    session.Store(xs.[i])
                    if i = 29 then session.SaveChanges()
                session.SaveChanges()
            (fun (session : IDocumentSession) -> storeMany' input session; input)


[<AutoOpen>]
module Raven = 

    let mutable private docStoreMap : Map<string,IDocumentStore> = Map.empty

    let getDocumentStore key = 
        match docStoreMap.TryFind key with
        | Some s -> s
        | None -> failwithf "No document store with key (%s) has been initailised" key

    let initialize key documentStore = 
        docStoreMap <- docStoreMap.Add(key,documentStore)

    let run key f = 
        use s = (getDocumentStore key).OpenSession()
        f(s)

    let runMultitenantSession key (db :string) f = 
        use s = (getDocumentStore key).OpenSession(db)
        f(s)

    let shutDownStore key = 
        (getDocumentStore key).Dispose()

    let shutDownAll() =
        Map.iter (fun _ (x : IDocumentStore) -> x.Dispose()) docStoreMap

    
    type RavenBuilder() = 
                                                                
         let bind sessionOp rest =
             (fun (s : IDocumentSession) -> rest (sessionOp s) s)

         member x.Bind(sessionOp, rest) = bind sessionOp rest
         member x.Return(a) = (fun _ -> a)
         member x.ReturnFrom(a : IDocumentSession -> 'a) = a    

    let raven = RavenBuilder()
