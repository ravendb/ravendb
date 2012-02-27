namespace Raven.Tests

open System
open Xunit
open FsUnit.Xunit
open Raven.Client.Linq
open Raven.Client
open Raven.Tests
open Raven.Abstractions.Extensions

type ``With Raven Computation Expression Builder``() as test =
    inherit RavenTest()

    [<Fact>]
    let ``Zero should allow empty else branch``() =
       use ds = test.NewDocumentStore()
       use session = ds.OpenSession()
       let called = ref false
       raven {
               if false then called := true 
             } |> run session
       Assert.False(!called)


    [<Fact>]
    let ``TryCatch should catch exception``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let called = ref false
        raven {
                try
                  failwith "Oh no!"
                with e ->
                  called := true 
              } |> run session

        Assert.True(!called)

    [<Fact>]
    let ``TryFinally with exception should execute finally``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let called = ref false
        let r = raven {
                      try
                        failwith "Oh No"
                      finally
                        called := true 
                     }
        
        Assert.Throws<Exception>(fun () -> r |> run session |> ignore) |> ignore
        Assert.True(!called)

    [<Fact>]
    let ``use should call Dispose``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let disposed = ref false
        let disp =
          { new IDisposable with
              member x.Dispose() = disposed := true }
    
        raven { use d = disp
                () 
              } |> run session
          
        Assert.True(!disposed)

    [<Fact>]
    let ``use! should call Dispose``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let disposed = ref false
        let disp = (fun s -> 
          { new IDisposable with
              member x.Dispose() = disposed := true })
    
        raven { use! d = disp
                () 
              } |> run session
          
        Assert.True(!disposed)

    [<Fact>]
    let ``While should loop and increment count``() =
      use ds = test.NewDocumentStore()
      use session = ds.OpenSession()
      let count = ref 0
      raven {
        while !count < 1 do
          incr count } |> run session
    
      
      Assert.Equal(1, !count)

    [<Fact>]
    let ``For should loop and increment count``() =
      use ds = test.NewDocumentStore()
      use session = ds.OpenSession()
      let count = ref 0
      raven {
        for i in [0;1] do incr count } |> run session

      Assert.Equal(2, !count)

    [<Fact>]
    let ``Combine should combine if statement and return true branch``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let expected = "Foo"
        let actual = 
            raven { 
                if true then () 
                return expected } |> run session
       
        Assert.Equal(expected, actual)
      
       


type ``Given a Initailised Document store execute using computation expression``() as test =
    inherit RavenTest()
    
    let createCustomers n = 
        Seq.init n (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq

    let storeMany (data : seq<'a>) =
        raven {
            for d in data do 
                do! store d >> ignore
            do! saveChanges
        }  
    
    [<Fact>]
    let ``Should be able to save and retrieve an entity``() =
           use ds = test.NewDocumentStore()
           use session = ds.OpenSession()
           let exp, act =
                raven {
                         let! expected = store (Customer.Create("test", new DateTime(2012, 1, 1)))
                         do! saveChanges
                         let! actual = (load<Customer> ["customers/test"] >> Seq.head) 
                         return expected, actual
                      } |> run session
           act |> should equal exp


    [<Fact>]
    let ``Should be able to query for filtered all entites``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let actual = 
                   raven {
                            do! storeMany (createCustomers 15) >> ignore
                            let! actual = query (where <@ fun x -> x.Dob < new DateTime(2012,1,7) @>)
                            return actual
                         } |> run session |> Seq.toList

        let expected = createCustomers ((new DateTime(2012,1,7)).Subtract(new DateTime(2012,1,1)).Days)

        actual |> should equal expected
    
    [<Fact>]
    let ``Should be able to query an entity including a reference``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let customer =  Customer.Create("Test", (new DateTime(2012, 1, 1)))
        let products = [| Product.Create("1-23-4", "Cat Food", 2.30) |]
        let order = Order.Create(customer.Id, products)

        let retrieved = 
            raven { 
                do! store customer >> ignore
                do! store order >> ignore
                do! saveChanges
                return! (fun s -> 
                              let order = ``include`` <@ fun s -> s.Customer @> (fun s -> s.Load("orders/1")) s
                              let customer : Customer = s.Load(order.Customer)
                              order, customer
                        )
            } |> run session

        fst(retrieved) |> should equal order
        snd(retrieved) |> should equal customer

    [<Fact>]
    let ``Should be able to query for filtered batches of entites``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual = 
            raven { 
                let! a = storeMany testData
                return! query (where <@ fun x -> x.Dob < new DateTime(2012,2,1) @>)
            } |> run session |> Seq.toList

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> Seq.toList

        actual |> should equal expected


    [<Fact>]
    let ``Should be able to compose queries``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let customer =  Customer.Create("Test", (new DateTime(2012, 1, 1)))
        let products = [| Product.Create("1-23-4", "Cat Food", 2.30) |]
        let order = Order.Create(customer.Id, products)

        let storeData = 
             raven { 
                    do! store customer >> ignore
                    do! store order >> ignore
                    do! saveChanges
                }

        let findOrderIncludeCustomer (id : string) = 
            raven { 
                 return! (fun s -> 
                              let order = ``include`` <@ fun s -> s.Customer @> (fun s -> s.Load(id)) s
                              let customer : Customer = s.Load(order.Customer)
                              order, customer
                        ) 
            }

        let retrieved = 
            raven { 
                do! storeData
                return! findOrderIncludeCustomer "orders/1"
            } |> run session

        fst(retrieved) |> should equal order
        snd(retrieved) |> should equal customer

    [<Fact>]
    let ``Should be able to query via a lucene query``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual = 
            raven { 
                let! a = storeMany testData
                return! luceneQuery (fun docQuery -> docQuery.WhereLessThan("Dob", new DateTime(2012,2,1)))
            } |> run session |> Seq.toList

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> Seq.toList

        Assert.Equal(expected, actual)
