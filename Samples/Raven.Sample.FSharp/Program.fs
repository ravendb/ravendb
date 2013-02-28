// Learn more about F# at http://fsharp.net


type Movie = 
    {
         Id:string
         Title:string
         Summary:string
    }


let create() = 
    let store = new Raven.Client.Document.DocumentStore()
    store.Url <- "http://localhost:8080/"
    store.Initialize() |> ignore

    use session = store.OpenSession()
    let x  =  {Id = null; Title ="Terminator 2"; Summary ="It was the end of the world"}
    session.Store(x)
    session.SaveChanges()

let read() = 
    let store = new Raven.Client.Document.DocumentStore()
    store.Url <- "http://localhost:8080/"
    store.Initialize() |> ignore

    use session = store.OpenSession()
    let m = session.Load<Movie>("movies/1")
    System.Console.WriteLine("{0} {1} {2}", m.Id, m.Title, m.Summary)




create()
read()
