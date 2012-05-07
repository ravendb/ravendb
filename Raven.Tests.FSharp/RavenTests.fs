namespace Raven.Tests

open System
open System.Linq
open Xunit
open Raven.Client.Linq
open Raven.Client
open Raven.Tests
open Raven.Abstractions.Extensions

type ``With Raven Computation Expression Builder``() =
    inherit RavenTest()

    [<Fact>]
    member test.``Zero should allow empty else branch``() =
       use ds = test.NewDocumentStore()
       use session = ds.OpenSession()
       let called = ref false
       raven {
               if false then called := true 
             } |> run session
       Assert.False(!called)


    [<Fact>]
    member test.``TryCatch should catch exception``() = 
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
    member test.``TryFinally with exception should execute finally``() =
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
    member test.``use should call Dispose``() =
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
    member test.``use! should call Dispose``() =
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
    member test.``While should loop and increment count``() =
      use ds = test.NewDocumentStore()
      use session = ds.OpenSession()
      let count = ref 0
      raven {
        while !count < 1 do
          incr count } |> run session
    
      
      Assert.Equal(1, !count)

    [<Fact>]
    member test.``For should loop and increment count``() =
      use ds = test.NewDocumentStore()
      use session = ds.OpenSession()
      let count = ref 0
      raven {
        for i in [0;1] do incr count } |> run session

      Assert.Equal(2, !count)

    [<Fact>]
    member test.``Combine should combine if statement and return true branch``() =
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let expected = "Foo"
        let actual = 
            raven { 
                if true then () 
                return expected } |> run session
       
        Assert.True((expected = actual))
      
       


type ``Given a Initailised Document store execute using computation expression``() as test =
    inherit RavenTest()
    
    let createCustomers n = 
        Seq.init n (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq

    let storeMany (data : seq<'a>) =
        raven {
            let results = ref []
            for d in data do
                let! a = store d 
                results :=  a :: !results
            return (!results |> List.rev)
        }  
    
    [<Fact>]
    let ``Should be able to skip results with skip``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let actual = 
                   raven {
                            do! storeMany (createCustomers 15) >> ignore
                            let! actual = query (where <@ fun x -> x.Dob < new DateTime(2012,1,7) @> >> skip 3)
                            return actual
                         } |> run session |> Seq.toList

        let expected = createCustomers ((new DateTime(2012,1,7)).Subtract(new DateTime(2012,1,1)).Days) |> Seq.skip 3 |> Seq.toList

        Assert.True((expected = actual))

    [<Fact>]
    let ``Should be able to take n results with take``() = 
        use ds = test.NewDocumentStore()
        use session = ds.OpenSession()
        let actual = 
                   raven {
                            do! storeMany (createCustomers 15) >> ignore
                            let! actual = query (where <@ fun x -> x.Dob < new DateTime(2012,1,7) @> >> take 3)
                            return actual
                         } |> run session |> Seq.toList

        let expected = createCustomers ((new DateTime(2012,1,4)).Subtract(new DateTime(2012,1,1)).Days) |> Seq.toList

        Assert.True((expected = actual))

    [<Fact>]
    let ``Should be able to project a property with select``() = 
        use ds = test.NewDocumentStore()
        ds.Conventions.DefaultQueryingConsistency <- Raven.Client.Document.ConsistencyOptions.QueryYourWrites
        use session = ds.OpenSession()
        let actual = 
                   raven {
                            do! storeMany (createCustomers 7) >> ignore
                            let! actual = query (select <@ fun x -> x.Id @>)  
                            return actual
                         } |> run session |> Seq.toList

        let expected = createCustomers 7 |> Seq.map (fun x -> x.Id) |> Seq.toList

        Assert.True((expected = actual))

    [<Fact>]
    let ``Should implicitly call save changes if runs to completion``() = 
           use ds = test.NewDocumentStore()
           use session = ds.OpenSession()
           let expected : Customer list= storeMany (createCustomers 15) |> run session
           let actual : Customer list = session.Query<Customer>().Customize(fun x->x.WaitForNonStaleResults() |> ignore).AsEnumerable() |> Seq.toList

           //Humm... Is this the only way I could get it too compile, Assert.Equals cant resolve the correct overload
           Assert.True((expected = actual))
    
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
           Assert.Equal(exp,act)
    
    [<Fact>]
    let ``Should be able to save and retrieve an entity with tryLoad``() =
           use ds = test.NewDocumentStore()
           use session = ds.OpenSession()
           let cust, act =
                raven {
                         let! customer = store (Customer.Create("test", new DateTime(2012, 1, 1)))
                         do! saveChanges
                         let! actual = (tryLoad<Customer> ["customers/test";"customers/doesntexist"]) 
                         return customer, actual
                      } |> run session
           let exp = 
                [
                  ("customers/test", Option.Some(cust));
                  ("customers/doesntexist", None)
                ]
           Assert.True((exp = (act |> Seq.toList)))

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

        Assert.True((expected = actual))
    
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
                              let order = including <@ fun s -> s.Customer @> (fun s -> s.Load("orders/1")) s
                              let customer : Customer = s.Load(order.Customer)
                              order, customer
                        )
            } |> run session

        Assert.Equal(order,fst(retrieved))
        Assert.Equal(customer,snd(retrieved))

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

        Assert.True((expected = actual))


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
                              let order = including <@ fun s -> s.Customer @> (fun s -> s.Load(id)) s
                              let customer : Customer = s.Load(order.Customer)
                              order, customer
                        ) 
            }

        let retrieved = 
            raven { 
                do! storeData
                return! findOrderIncludeCustomer "orders/1"
            } |> run session

        Assert.Equal(order,fst(retrieved))
        Assert.Equal(customer,snd(retrieved))

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

        Assert.True((expected = actual))
