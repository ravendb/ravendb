namespace Raven.Client

open System
open System.Linq
open System.Linq.Expressions
open System.ComponentModel.Composition

open Raven.Client
open Raven.Client.Indexes
open Raven.Client.Linq
open Raven.Client.Document
open Raven.Abstractions.Extensions
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Linq
open Raven.Imports.Newtonsoft.Json

 [<AutoOpen>]
 module Operators = 
        
        let private toOption<'a> (a : 'a) = 
            match box a with
            | null -> None
            | _ -> Some(a)

        let query (f : (IRavenQueryable<'a> -> #IQueryable<'b>)) (a : IDocumentSession) = 
            f(a.Query<'a>()).AsEnumerable()
        
        let luceneQuery<'a, 'b> (f : (IDocumentQuery<'a> -> IDocumentQuery<'b>)) (a : IDocumentSession) = 
            f(a.Advanced.LuceneQuery<'a>()).AsEnumerable()
        
        let select (p : Expr<'a -> 'b>) (a : IRavenQueryable<'a>) =
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,'b>>(a, b |> Array.ofSeq)) p
            a.Select(expr)

        let skip n (a : IRavenQueryable<'a>) = 
            a.Skip(n)

        let take n (a : IRavenQueryable<'a>) = 
            a.Take(n)

        let queryIndex<'a, 'b, 'index when 'index : (new : unit -> 'index) and 'index :> AbstractIndexCreationTask> (f : (IRavenQueryable<'a> -> IQueryable<'b>)) (a : IDocumentSession) =
            f(a.Query<'a, 'index>()).AsEnumerable()

        let where (p : Expr<'a -> bool>) (a : IRavenQueryable<'a>) =
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,bool>>(a, b |> Array.ofSeq)) p
            a.Where(expr)

        let orderBy descending (p : Expr<'a -> 'b>) (a : IRavenQueryable<'a>) =
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,'b>>(a, b |> Array.ofSeq)) p
            if descending 
            then a.OrderByDescending(expr)
            else a.OrderBy(expr)

        let including (p : Expr<'a -> 'b>) (loader : ILoaderWithInclude<'a> -> 'c) (a : IDocumentSession) = 
            let expr = Linq.toLinqExpression (fun b a -> Expression.Lambda<Func<'a,obj>>(a, b |> Array.ofSeq)) p
            loader(a.Include<'a>(expr))
        
        let putAttachment documentId etag data metaData (a : IDocumentSession) = 
            a.Advanced.DocumentStore.DatabaseCommands.PutAttachment(documentId, etag, data, Raven.Json.Linq.RavenJObject.FromObject(metaData))

        let createAttachment documentId data metaData (a : IDocumentSession) =
            putAttachment documentId (Nullable()) data metaData a

        let getAttachmentAsStream<'a> documentId (a : IDocumentSession) =
            let attachment = a.Advanced.DocumentStore.DatabaseCommands.GetAttachment(documentId)
            (attachment.Metadata.JsonDeserialization<'a>(), attachment.Etag, attachment.Data.Invoke())

        let getAttachmentAsBytes<'a> documentId (a : IDocumentSession) =
            let attachment = a.Advanced.DocumentStore.DatabaseCommands.GetAttachment(documentId)
            let attachmentBody = attachment.Data.Invoke().ReadData()
            (attachment.Metadata.JsonDeserialization<'a>(), attachment.Etag, attachmentBody)
        
        let tryLoad<'a> (id : seq<string>) (a : IDocumentSession) = 
            a.Load<'a>(id) |> Seq.map2 (fun id result -> (id, toOption result)) id
        
        let load<'a> (id : seq<string>) (a : IDocumentSession) = 
            a.Load(id).Cast<'a>()

        let store input  = 
            (fun (s : IDocumentSession) -> s.Store(input); input)
        
        let delete (input ) = 
            (fun (session : IDocumentSession) ->  session.Delete(input); input)

        let saveChanges (s : IDocumentSession) = 
            s.SaveChanges()

[<AutoOpen>]
module Raven = 

    let run (session : IDocumentSession) f =
       let a = f(session)
       session.SaveChanges()
       a

    type RavenFunc<'a> = (IDocumentSession -> 'a)

    type RavenBuilder() = 
                                                                
         let bind sessionOp rest : RavenFunc<_> =
             (fun (s : IDocumentSession) -> rest (sessionOp s) s)
         
         let ret a : RavenFunc<_> = (fun _ -> a)

         let delay f : RavenFunc<_> = bind (ret ()) f

         member x.Bind(sessionOp, rest) = bind sessionOp rest

         member x.Return(a) =  ret a

         member x.ReturnFrom(a : RavenFunc<_>) = a

         member x.Yield(a) =  ret a

         member x.YieldFrom(a : RavenFunc<_>) = a
         
         member x.Delay(f) = delay f

         member x.Zero() = x.Return ()

         member x.Combine(a, b) =
            bind a (fun () -> b)

         member x.TryWith(sessionOp : IDocumentSession -> 'a, handler) =
            (fun s -> try 
                        sessionOp s 
                      with e -> 
                        (handler e s))

         member x.TryFinally(sessionOp: IDocumentSession -> 'a, compensation) =
           (fun s -> try 
                        sessionOp s 
                     finally 
                        compensation())

         member x.While(guard, sessionOp) =
            if guard() then
              bind sessionOp (fun () -> x.While(guard,sessionOp))
            else
              x.Zero()

         member x.Using(resource:#IDisposable, f) =
            x.TryFinally(f resource, (fun () -> match resource with null -> () | disp -> disp.Dispose()))

         member x.For(sequence : seq<_>, f) =
            x.Using(sequence.GetEnumerator(),
              (fun enum ->
                 x.While(
                   (fun () -> enum.MoveNext()),
                   x.Delay(fun () -> f enum.Current))))


    let raven = RavenBuilder()
