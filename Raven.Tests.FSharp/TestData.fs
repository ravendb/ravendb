namespace Raven.Tests

open System

type Product = {
    mutable Id : string
    Name : string
    Price : float
}
with 
    static member Create(id, name : string, price : float) =
        { Id = "products/"+id; Name = name; Price = price }


type Order = {
       mutable Id : string
       Date : DateTimeOffset
       Customer : string
       Items : Product array
}
with 
    static member Create(customer : string, ?items) =
        { Id = null; Customer = customer; Date = DateTimeOffset.Now; Items = defaultArg items [||] }

type CustomerAttachmentMetaData = {
    Description : string
}

type Customer = {
       mutable Id : string
       Name : string 
       Dob : DateTime
}
with 
    static member Create(name : string, dob : DateTime) =
        { Id = "customers/"+name; Name = name; Dob = dob } 

