// using System.Text;
// using Sparrow.Json;
//
// namespace FastTests.SchemaValidation;
//
// internal class TypeSchemaRuleValidator(TypeRestriction type, string property = null) : ISchemaRuleValidator
// {
//     public void Validate(object current, string path, StringBuilder errorBuilder)
//     {
//         TypeRestriction? actualType = null;
//         switch (current) {
//             case BlittableJsonReaderObject _:
//                 actualType = TypeRestriction.@object;
//                 break;
//         }   
//
//         if (actualType != type)
//             errorBuilder.AppendLine($"{path} should be of type {type} but actual type is {actualType}");
//     }
// }

namespace Raven.Server.SchemaValidation;