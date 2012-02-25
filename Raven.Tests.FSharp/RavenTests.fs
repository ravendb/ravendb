namespace Raven.Tests

open System
open Xunit
open FsUnit.Xunit
open Raven.Client.Linq
open Raven.Client
open Raven.Tests
open Raven.Abstractions.Extensions

type ``Given a Initailised Document store execute using computation expression``() =
    inherit RavenTest()
    
    let raven = RavenBuilder()

    let createCustomers n = 
        Seq.init n (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
    
    [<Fact>]
    member test.``Should be able to save and retrieve an entity``() =
           let exp, act =
                raven {
                         let! expected = storeImmediate (Customer.Create("test", new DateTime(2012, 1, 1)))
                         let! actual = (load<Customer> ["customers/test"] >> Seq.head) 
                         return expected, actual
                      } |> run "raven"
           act |> should equal exp

    [<Fact>]
    member test.``Should be able to save and retrieve batches of entites``() =
           let exp, act =
                raven {
                         let testData = createCustomers 60
                         let! expected = storeMany testData
                         let! actual = queryAll<Customer> >> List.ofSeq
                         return expected, actual
                      } |> run "raven"
           act |> should equal exp

    [<Fact>]
    member test.``Should be able to query for filtered all entites``() = 
        let actual : seq<Customer> = 
                   raven {
                            do! storeMany (createCustomers 365) >> ignore
                            let! actual = queryAllBy (where <@ fun x -> x.Dob < new DateTime(2012,8,1) @>)
                            return actual
                         } |> run "raven"

        let expected = createCustomers ((new DateTime(2012,8,1)).Subtract(new DateTime(2012,1,1)).Days)

        actual |> should equal expected
    
    [<Fact>]
    member test.``Should be able to query an entity including a reference``() =
        let customer =  Customer.Create("Test", (new DateTime(2012, 1, 1)))
        let products = [| Product.Create("1-23-4", "Cat Food", 2.30) |]
        let order = Order.Create(customer.Id, products)

        let retrieved = 
            raven { 
                do! store customer >> ignore
                do! store order >> ignore
                do! commit
                return! (fun s -> 
                              let order = ``include`` <@ fun s -> s.Customer @> (fun s -> s.Load("orders/1")) s
                              let customer : Customer = s.Load(order.Customer)
                              order, customer
                        )
            } |> run "raven"

        fst(retrieved) |> should equal order
        snd(retrieved) |> should equal customer

    [<Fact>]
    member test.``Should be able to query for filtered batches of entites``() = 
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual = 
            raven { 
                let! a = storeMany testData
                return! query (where <@ fun x -> x.Dob < new DateTime(2012,2,1) @>)
            } |> run "raven"

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float)))

        actual |> should equal expected

    [<Fact>]
    member test.``Should be able to compose queries``() =
        let customer =  Customer.Create("Test", (new DateTime(2012, 1, 1)))
        let products = [| Product.Create("1-23-4", "Cat Food", 2.30) |]
        let order = Order.Create(customer.Id, products)

        let storeData = 
             raven { 
                    do! store customer >> ignore
                    do! store order >> ignore
                    do! commit
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
            } |> run "raven"

        fst(retrieved) |> should equal order
        snd(retrieved) |> should equal customer

    [<Fact>]
    member test.``Should be able to query via a lucene query``() = 
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual : seq<Customer> = 
            raven { 
                let! a = storeMany testData
                return! luceneQuery (fun docQuery -> docQuery.WhereLessThan("Dob", new DateTime(2012,2,1)))
            } |> run "raven"

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float)))

        actual |> should equal expected

type ``Given a Initailised Document store execute using combinators``() =
    inherit RavenTest()
    
    [<Fact>]
    member test.``Should be able to save and retrieve an entity``() =
           let expected = Customer.Create("test", new DateTime(2012, 1, 1))
           run "raven" (storeImmediate expected) |> ignore
           let actual : Customer = run "raven" (fun s -> s.Load("customers/test"))
           actual |> should equal expected


    [<Fact>]
    member test.``Should be able to save and retrieve batches of entites``() = 
          let expected = 
            Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float)))
            |> List.ofSeq
          (storeMany expected) |> run "raven" |> ignore
          let actual : Customer list = run "raven" queryAll |> List.ofSeq
          actual |> should equal expected

    [<Fact>]
    member test.``Should be able to query for filtered all entites``() = 
        let testData = Seq.init 365 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        (storeMany testData) |> run "raven" |> ignore 
        let actual : seq<Customer> = queryAllBy (where <@ fun x -> x.Dob < new DateTime(2012,8,1) @>) |> run "raven"
        let total = (new DateTime(2012,8,1)).Subtract(new DateTime(2012,1,1)).Days
        let expected = Seq.init total (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float)))
        actual |> should equal expected
    
    [<Fact>]
    member test.``Should be able to query an entity including a reference``() = 
        let customer =  Customer.Create("Test", (new DateTime(2012, 1, 1)))
        let products = [| Product.Create("1-23-4", "Cat Food", 2.30) |]
        let order = Order.Create(customer.Id, products)
        (storeImmediate customer) |> run "raven" |> ignore
        (storeImmediate order) |> run "raven" |> ignore
        let retrieved : Order * Customer = 
            run "raven" (fun s -> 
                                        let order = ``include`` <@ fun s -> s.Customer @> (fun s -> s.Load("orders/1")) s
                                        let customer : Customer = s.Load(order.Customer)
                                        order, customer
                                 )
        fst(retrieved) |> should equal order
        snd(retrieved) |> should equal customer



    [<Fact>]
    member test.``Should be able to query for filtered batches of entites``() = 
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        run "raven" (storeMany testData) |> ignore
        let actual : seq<Customer> =  query (where <@ fun x -> x.Dob < new DateTime(2012,2,1) @>) |> run "raven"
        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float)))
        actual |> should equal expected


    [<Fact>]
    member test.``Should be able to put an attachment to Raven``() = 
        let customer = run "raven" (storeImmediate (Customer.Create("test", new DateTime(2012, 1, 1))))
        let attachmentStream = new IO.MemoryStream(Seq.init 100 (fun x -> 1uy) |> Seq.toArray)
        run "raven" (createAttachment customer.Id attachmentStream {Description = "description"})


    [<Fact>]
    member test.``Should be able to retrieve an attachment from raven as bytes``() =
        let customer = run "raven" (storeImmediate (Customer.Create("test", new DateTime(2012, 1, 1))))
        let attachmentStream = new IO.MemoryStream(Seq.init 100 (fun x -> 1uy) |> Seq.toArray)
        run "raven" (createAttachment customer.Id attachmentStream {Description = "description"})

        let metaData, etag, body = run "raven" (getAttachmentAsBytes<CustomerAttachmentMetaData> customer.Id)
        
        let expectedbody = Seq.init 100 (fun x -> 1uy) |> Seq.toArray
        let expectedMetaData = {Description = "description"}

        metaData |> should equal expectedMetaData
        body |> should equal expectedbody

    [<Fact>]
    member test.``Should be able to retrieve an attachment from raven as stream``() =
        let customer = run "raven" (storeImmediate (Customer.Create("test", new DateTime(2012, 1, 1))))
        let attachmentStream = new IO.MemoryStream(Seq.init 100 (fun x -> 1uy) |> Seq.toArray)
        run "raven" (createAttachment customer.Id attachmentStream {Description = "description"})

        let ms = new IO.MemoryStream()
        let metaData, etag = run "raven" (getAttachmentAsStream<CustomerAttachmentMetaData> customer.Id ms)
        
        let expectedbody = Seq.init 100 (fun x -> 1uy) |> Seq.toArray
        let expectedMetaData = {Description = "description"}

        metaData |> should equal expectedMetaData
        ms.ToArray() |> should equal expectedbody
      

        