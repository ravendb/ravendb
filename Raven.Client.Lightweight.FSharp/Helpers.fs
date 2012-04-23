namespace Raven.Client

open System
open System.Linq
open System.Linq.Expressions
open Raven.Imports.Newtonsoft.Json
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Linq

type UnionTypeConverter() =
    inherit JsonConverter()

    let doRead pos (reader: JsonReader) = 
        reader.Read() |> ignore 

    override x.CanConvert(typ:Type) =
        let result = 
            ((typ.GetInterface(typeof<System.Collections.IEnumerable>.FullName) = null) 
            && FSharpType.IsUnion typ)
        result

    override x.WriteJson(writer: JsonWriter, value: obj, serializer: JsonSerializer) =
        let t = value.GetType()
        let write (name : string) (fields : obj []) = 
            writer.WriteStartObject()
            writer.WritePropertyName("case")
            writer.WriteValue(name)  
            writer.WritePropertyName("values")
            serializer.Serialize(writer, fields)
            writer.WriteEndObject()   

        let (info, fields) = FSharpValue.GetUnionFields(value, t)
        write info.Name fields

    override x.ReadJson(reader: JsonReader, objectType: Type, existingValue: obj, serializer: JsonSerializer) =      
         let cases = FSharpType.GetUnionCases(objectType)
         if reader.TokenType <> JsonToken.Null  
         then 
            doRead "1" reader
            doRead "2" reader
            let case = cases |> Array.find(fun x -> x.Name = if reader.Value = null then "None" else reader.Value.ToString())
            doRead "3" reader
            doRead "4" reader
            doRead "5" reader
            let fields =  [| 
                   for field in case.GetFields() do
                       let result = serializer.Deserialize(reader, field.PropertyType)
                       reader.Read() |> ignore
                       yield result
             |] 
            let result = FSharpValue.MakeUnion(case, fields)
            while reader.TokenType <> JsonToken.EndObject do
                doRead "6" reader         
            result
         else
            FSharpValue.MakeUnion(cases.[0], [||]) 

module Linq = 
    
    ///From http://stackoverflow.com/questions/2682475/converting-f-quotations-into-linq-expressions
    let toLinqExpression (f : ParameterExpression list -> Expression -> Expression<'a>) p =
        let rec translateExpr (linq:Expression) = 
            match linq with
            | :? MethodCallExpression as mc ->
                let le = mc.Arguments.[0] :?> LambdaExpression
                let args, body = translateExpr le.Body
                le.Parameters.[0] :: args, body
            | _ -> [], linq
        let args,body = translateExpr (QuotationEvaluator.ToLinqExpression p)
        f args body

