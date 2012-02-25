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
           let ds = test.NewDocumentStore()
           initialize "raven" ds
           let exp, act =
                raven {
                         let! expected = storeImmediate (Customer.Create("test", new DateTime(2012, 1, 1)))
                         let! actual = (load<Customer> ["customers/test"] >> Seq.head) 
                         return expected, actual
                      } |> run "raven"
           act |> should equal exp
           shutDownAll()


    [<Fact>]
    member test.``Should be able to save and retrieve batches of entites``() =
           let ds = test.NewDocumentStore()
           initialize "raven" ds
           let exp, act =
                raven {
                         let testData = createCustomers 60
                         let! expected = storeMany testData
                         let! actual = queryAll<Customer> >> List.ofSeq
                         return expected, actual
                      } |> run "raven"
           act |> should equal exp
           shutDownAll()

    [<Fact>]
    member test.``Should be able to query for filtered all entites``() = 
        let ds = test.NewDocumentStore()
        initialize "raven" ds
        let actual = 
                   raven {
                            do! storeMany (createCustomers 365) >> ignore
                            let! actual = queryAllBy (where <@ fun x -> x.Dob < new DateTime(2012,8,1) @>)
                            return actual
                         } |> run "raven" |> Seq.toList

        let expected = createCustomers ((new DateTime(2012,8,1)).Subtract(new DateTime(2012,1,1)).Days)

        actual |> should equal expected
        shutDownAll()
    
    [<Fact>]
    member test.``Should be able to query an entity including a reference``() =
        let ds = test.NewDocumentStore()
        initialize "raven" ds
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
        shutDownAll()

    [<Fact>]
    member test.``Should be able to query for filtered batches of entites``() = 
        let ds = test.NewDocumentStore()
        initialize "raven" ds
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual = 
            raven { 
                let! a = storeMany testData
                return! query (where <@ fun x -> x.Dob < new DateTime(2012,2,1) @>)
            } |> run "raven" |> Seq.toList

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> Seq.toList

        actual |> should equal expected
        shutDownAll()

    [<Fact>]
    member test.``Should be able to compose queries``() =
        let ds = test.NewDocumentStore()
        initialize "raven" ds
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
        shutDownAll()

    [<Fact>]
    member test.``Should be able to query via a lucene query``() = 
        let ds = test.NewDocumentStore()
        initialize "raven" ds
        let testData = Seq.init 60 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> List.ofSeq
        
        let actual = 
            raven { 
                let! a = storeMany testData
                return! luceneQuery (fun docQuery -> docQuery.WhereLessThan("Dob", new DateTime(2012,2,1)))
            } |> run "raven" |> Seq.toList

        let expected = Seq.init 31 (fun i -> Customer.Create("test_"+i.ToString(), (new DateTime(2012, 1, 1)).AddDays(i |> float))) |> Seq.toList

        Assert.Equal(expected, actual)
        shutDownAll()
