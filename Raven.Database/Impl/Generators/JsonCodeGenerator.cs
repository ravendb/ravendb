using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Impl.Generators
{
    public class JsonCodeGenerator
    {
        private string language;

        internal class FieldType
        {
            public readonly string Name;
            public readonly bool IsPrimitive;
            public readonly bool IsArray;

            public FieldType(Type type, bool isArray = false)
            {
                // There should be a better way to do this.
                if (type == typeof(bool))
                    this.Name = "bool";
                else if (type == typeof(long))
                    this.Name = "long";
                else if (type == typeof(int))
                    this.Name = "int";
                else if (type == typeof(string))
                    this.Name = "string";
                else if (type == typeof(float))
                    this.Name = "float";
                else if (type == typeof(double))
                    this.Name = "double";
                else if (type == typeof(object))
                    this.Name = "object";
                else
                    this.Name = type.Name;

                this.IsPrimitive = true;
                this.IsArray = isArray;
            }

            public FieldType(string type, bool isArray = false, bool isPrimitive = false)
            {
                this.Name = type;
                this.IsPrimitive = isPrimitive;
                this.IsArray = isArray;
            }
        }

        internal class ClassType : FieldType
        {
            private static IDictionary<string, FieldType> NoProperties = new Dictionary<string, FieldType>();

            public readonly IDictionary<string, FieldType> Properties;
            
            public ClassType( string name, IDictionary<string, FieldType> properties = null ) : base ( name, false )
            {
                this.Properties = properties;
                if (this.Properties == null)
                    this.Properties = NoProperties;
            }
        }

        public JsonCodeGenerator(string lang)
        {
            if (string.IsNullOrWhiteSpace(lang))
                throw new ArgumentNullException("lang");

            this.language = lang;
        }

        public string Execute(JsonDocument document)
        {
            if (document == null)
                throw new ArgumentNullException("document");

            var classes = GenerateClassTypesFromObject("Class", document.DataAsJson);

            var list = classes.ToList();

            throw new NotImplementedException();
        }

        internal IEnumerable<ClassType> GenerateClassTypesFromObject(string name, RavenJObject @object)
        {
            var fields = new Dictionary<string, FieldType>();

            foreach (var pair in @object)
            {
                switch (pair.Value.Type)
                {
                    // Inner objects.
                    case JTokenType.Object:
                        var type = GenerateClassTypesFromObject(pair.Key + "Class", (RavenJObject)pair.Value).Single();                        
                        fields[pair.Key] = new FieldType( type.Name, type.IsArray );
                        yield return type;
                        break;
                    case JTokenType.Array:
                        var guessedType = GuessTokenTypeFromArray(pair.Key, (RavenJArray)(pair.Value));
                        if (guessedType is ClassType)
                        {
                            // We will defer the analysis and create a new name;
                            fields[pair.Key] = new FieldType(guessedType.Name, true);

                            // Add the token to be inspected individually.                            
                            yield return (ClassType)guessedType;
                        }
                        else
                        {
                            fields[pair.Key] = new FieldType(guessedType.Name, true, true);
                        }
                        break;

                    default:
                        fields[pair.Key] = GetTokenTypeFromPrimitiveType(pair.Value);
                        break;
                }
            }  

            // Check if we can get the name from the metadata. 
            yield return new ClassType(name, fields);
        }

        private FieldType GuessTokenTypeFromArray(string name, RavenJArray array)
        {
            var token = array.First();

            switch (token.Type)
            {
                case JTokenType.Object:
                    return GenerateClassTypesFromObject(name, (RavenJObject)token).First();
                case JTokenType.Array:
                    return GuessTokenTypeFromArray(name, (RavenJArray)token);
                default:
                    return GetTokenTypeFromPrimitiveType(token);
            }

            throw new NotSupportedException("We shouldn't have hit this.");
        }

        private FieldType GetTokenTypeFromPrimitiveType(RavenJToken token)
        {
            switch (token.Type)
            {
                // Base types.
                case JTokenType.Boolean: return new FieldType(typeof(bool));
                case JTokenType.Bytes: return new FieldType("byte", true, true);

                case JTokenType.Date: return new FieldType(typeof(DateTimeOffset));

                case JTokenType.Guid: return new FieldType(typeof(Guid));
                case JTokenType.TimeSpan: return new FieldType(typeof(TimeSpan));
                case JTokenType.Uri: return new FieldType(typeof(Uri));
                case JTokenType.Float: return new FieldType(typeof(float));

                case JTokenType.Integer: // Could be integer or long.                
                case JTokenType.String: // Could be anything.
                case JTokenType.Undefined:
                    return GuessTokenTypeFromContent(token);

                // Could be anything.
                case JTokenType.Null:
                    return new FieldType(typeof(object));

                default:
                    throw new NotSupportedException("We shouldn't have hit this. This is a bug in the caller routine.");
            }
        }

        private FieldType GuessTokenTypeFromContent(RavenJToken token)
        {
            var content = token.Value<string>();

            if (ParseHelper.TryAction<bool>(x => bool.TryParse(content, out x)))
                return new FieldType(typeof(bool));
            if (ParseHelper.TryAction<int>(x => int.TryParse(content, out x)))
                return new FieldType(typeof(int));
            if (ParseHelper.TryAction<long>(x => long.TryParse(content, out x)))
                return new FieldType(typeof(long));

            if (ParseHelper.TryAction<float>(x => float.TryParse(content, out x)))
                return new FieldType(typeof(float));
            if (ParseHelper.TryAction<double>(x => double.TryParse(content, out x)))
                return new FieldType(typeof(double));

            if (ParseHelper.TryAction<Guid>(x => Guid.TryParse(content, out x)))
                return new FieldType(typeof(Guid));
            if (ParseHelper.TryAction<TimeSpan>(x => TimeSpan.TryParse(content, out x)))
                return new FieldType(typeof(TimeSpan));
            if (ParseHelper.TryAction<DateTimeOffset>(x => DateTimeOffset.TryParse(content, out x)))
                return new FieldType(typeof(DateTimeOffset));

            
            if (Uri.IsWellFormedUriString(content, UriKind.Absolute))
                return new FieldType(typeof(Uri));

            return new FieldType(typeof(string));
        }

        private static class ParseHelper
        {
            public static bool TryAction<T> ( Func<T, bool> action )
            {
                T t = default(T);
                return action(t);
            }
        }


        private const string codeLayout = @"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

##namespace
{
##code
}
";

    }
}
