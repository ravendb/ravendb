using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Raven.Database.Impl.Generators
{
    public class JsonCodeGenerator
    {
        private string language;


        private Lazy<IDictionary<Type, FieldType>> _knownTypes = new Lazy<IDictionary<Type, FieldType>>(InitializeKnownTypes, true);

        internal IDictionary<Type, FieldType> KnownTypes
        {
            get { return _knownTypes.Value; }
        }

        private readonly IDictionary<string, ClassType> _generatedTypes = new Dictionary<string, ClassType>();


        private static IDictionary<Type, FieldType> InitializeKnownTypes()
        {
            var types = new Dictionary<Type, FieldType>();
            types[typeof(bool)] = new FieldType("bool", false, true);
            types[typeof(long)] = new FieldType("long", false, true);
            types[typeof(int)] = new FieldType("int", false, true);
            types[typeof(string)] = new FieldType("string", false, true);
            types[typeof(float)] = new FieldType("float", false, true);
            types[typeof(double)] = new FieldType("double", false, true);
            types[typeof(object)] = new FieldType("object", false, true);
            types[typeof(byte[])] = new FieldType("byte", true, true);
            types[typeof(int[])] = new FieldType("int", true, true);
            types[typeof(Guid)] = new FieldType(typeof(Guid));
            types[typeof(DateTime)] = new FieldType(typeof(DateTimeOffset).Name, false, true);
            types[typeof(DateTimeOffset)] = new FieldType(typeof(DateTimeOffset));
            types[typeof(TimeSpan)] = new FieldType(typeof(TimeSpan));
            types[typeof(Uri)] = new FieldType(typeof(Uri));

            return types;
        }


        internal class FieldType
        {
            public readonly string Name;
            public readonly bool IsPrimitive;
            public readonly bool IsArray;

            public FieldType(Type type, bool isArray = false)
            { 
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

            public override bool Equals(object obj)
            {
                // If parameter cannot be cast to FieldType return false:
                FieldType p = obj as FieldType;
                if (p == null)
                    return false;

                // Return true if the fields match:
                return this == p;
            }

            public bool Equals(FieldType p)
            {
                // Return true if the fields match:
                return this == p;
            }

            public override int GetHashCode()
            {
                unchecked // Overflow is fine, just wrap
                {
                    int hash = 17;
                    // Suitable nullity checks etc, of course :)
                    hash = hash * 23 + Name.GetHashCode();
                    hash = hash * 23 + IsPrimitive.GetHashCode();
                    hash = hash * 23 + IsArray.GetHashCode();
                    return hash;
                }
            }

            public static bool operator ==(FieldType a, FieldType b)
            {
                // If both are null, or both are same instance, return true.
                if (Object.ReferenceEquals(a, b))
                {
                    return true;
                }

                // If one is null, but not both, return false.
                if (((object)a == null) || ((object)b == null))
                {
                    return false;
                }

                // Return true if the fields match:
                return a.Name == b.Name && a.IsPrimitive == b.IsPrimitive && a.IsArray == b.IsArray;
            }

            public static bool operator !=(FieldType a, FieldType b)
            {
                return !(a == b);
            }
        }

        internal class ClassType : FieldType
        {
            private static IDictionary<string, FieldType> NoProperties = new Dictionary<string, FieldType>();

            public readonly IDictionary<string, FieldType> Properties;
            
            public ClassType( string name, IDictionary<string, FieldType> properties = null ) : base ( name, false )
            {
                this.Properties = new ReadOnlyDictionary<string, FieldType>(properties);
            }

            public static bool operator ==(ClassType a, ClassType b)
            {
                // If both are null, or both are same instance, return true.
                if (Object.ReferenceEquals(a, b))
                    return true;

                // If one is null, but not both, return false.
                if (((object)a == null) || ((object)b == null))
                {
                    return false;
                }

                return Compare<string, FieldType>(a.Properties, b.Properties);
            }

            public static bool operator !=(ClassType a, ClassType b)
            {
                return !(a == b);
            }

            public override bool Equals(object obj)
            {
                // If parameter cannot be cast to FieldType return false:
                ClassType p = obj as ClassType;
                if (p == null)
                    return false;

                // Return true if the fields match:
                return this == p;
            }

            public override int GetHashCode()
            {
                unchecked // Overflow is fine, just wrap
                {
                    int hash = 17;

                    // Suitable nullity checks etc, of course :)
                    hash = hash * 23 + IsPrimitive.GetHashCode();
                    hash = hash * 23 + IsArray.GetHashCode();
                    hash = hash * 23 + Properties.GetHashCode();

                    return hash;
                }
            }

            private static bool Compare<TKey, TValue>(IDictionary<TKey, TValue> dict1, IDictionary<TKey, TValue> dict2)
            {
                if (dict1 == dict2) return true;
                if ((dict1 == null) || (dict2 == null)) return false;
                if (dict1.Count != dict2.Count) return false;

                var comparer = EqualityComparer<TValue>.Default;

                foreach (KeyValuePair<TKey, TValue> kvp in dict1)
                {
                    TValue value2;
                    if (!dict2.TryGetValue(kvp.Key, out value2)) 
                        return false;                    

                    if (!comparer.Equals(kvp.Value, value2)) 
                        return false;
                }
                return true;
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

            var @class = "Class";
            var @namespace = "Unknown"; 

            var metadata = document.Metadata;
            if ( metadata != null )
            {
                // Retrieve the class and metadata if available.
                // "Raven-Clr-Type": "Namespace.ClassName, AssemblyName"
                RavenJToken typeToken;
                if ( metadata.TryGetValue( "Raven-Clr-Type", out typeToken ) )
                {
                    var values = typeToken.Value<string>().Split(',');
                    if ( values.Length == 2 )
                    {
                        var data = values[0];
                        int index = data.LastIndexOf('.');
                        if ( index > 0 )
                        {
                            var potentialClass = data.Substring(index + 1);
                            if (potentialClass.Length > 0)
                                @class = potentialClass;

                            var potentialNamespace = data.Substring(0, index);
                            if (potentialNamespace.Length > 0)
                                @namespace = potentialNamespace;
                        }                        
                    }
                }
            }

            var classes = GenerateClassesTypesFromObject(@class, document.DataAsJson);                                

            string classCode = GenerateClassCodeFromSpec(classes);

            var code = codeLayout.Replace("##namespace", @namespace)
                                 .Replace("##code", classCode);

            return code;
        }

        private string GenerateClassCodeFromSpec(IEnumerable<ClassType> classes)
        {
            var codeBuilder = new StringBuilder();
            
            foreach( var @class in classes )
            {
                codeBuilder.Append("\tpublic class " + @class.Name + Environment.NewLine);
                codeBuilder.Append("\t{" + Environment.NewLine);

                foreach ( var @field in @class.Properties )
                {
                    codeBuilder.Append("\t\tpublic " + field.Value.Name);
                    codeBuilder.Append(field.Value.IsArray ? "[] " : " ");
                    codeBuilder.Append(field.Key + " { get; set; } ");
                    codeBuilder.Append(Environment.NewLine);
                }

                codeBuilder.Append("\t}" + Environment.NewLine);
            }

            return codeBuilder.ToString();
        }

        internal IEnumerable<ClassType> GenerateClassesTypesFromObject(string name, RavenJObject @object)
        {
            // We need to clear the generated types;
            this._generatedTypes.Clear();

            // Repopulate the generated types after working on the object.
            var root = GenerateClassTypesFromObject(name, @object);

            foreach ( var pair in _generatedTypes.ToList())
            {
                var @class = pair.Value;

                bool changed = false;
                var properties = @class.Properties;
                foreach ( var fieldPair in properties.ToList() )
                {
                    var field = fieldPair.Value;
                    if (field.Name == root.Name)
                    {
                        properties[fieldPair.Key] = new FieldType(name, field.IsArray, field.IsPrimitive);
                        changed = true;
                    }                        
                }                

                if ( @class.Name == root.Name )
                {
                    _generatedTypes[pair.Key] = new ClassType(name, properties);
                }                    
                else if ( changed )
                {
                    _generatedTypes[pair.Key] = new ClassType(@class.Name, properties);
                }
            }

            // Return all the potential classes found.
            return this._generatedTypes.Select(x => x.Value).ToList();
        }

        internal ClassType GenerateClassTypesFromObject(string name, RavenJObject @object)
        {
            var fields = new Dictionary<string, FieldType>();

            foreach (var pair in @object)
            {
                switch (pair.Value.Type)
                {
                    // Inner objects.
                    case JTokenType.Object:
                        var type = GenerateClassTypesFromObject(pair.Key + "Class", (RavenJObject)pair.Value);                        
                        fields[pair.Key] = new FieldType(type.Name, type.IsArray);
                        break;
                    case JTokenType.Array:
                        var array = (RavenJArray) pair.Value;
                        if (array.Length == 0)
                        {
                            fields[pair.Key] = new FieldType("object", true, true);
                        }
                        else
                        {
                            var guessedType = GuessTokenTypeFromArray(pair.Key, array);
                            if (guessedType is ClassType)
                            {
                                // We will defer the analysis and create a new name;
                                fields[pair.Key] = new FieldType(guessedType.Name, true);
                            }
                            else
                            {
                                fields[pair.Key] = new FieldType(guessedType.Name, true, true);
                            }
                        }
                        
                        break;

                    default:
                        fields[pair.Key] = GetTokenTypeFromPrimitiveType(pair.Value);
                        break;
                }
            }  

            // Check if we can get the name from the metadata. 
            var clazz = new ClassType(name, fields);
            clazz = IncludeGeneratedClass(clazz);
            return clazz;            
        }

        private ClassType IncludeGeneratedClass(ClassType clazz)
        {
            var key = clazz.Name;

            ClassType dummy;
            int i = 1;
            while (_generatedTypes.TryGetValue(key, out dummy))
            {
                key = clazz.Name + i;
                i++;
            }

            clazz = new ClassType(key, clazz.Properties);

            foreach (var pair in _generatedTypes)
            {
                if (pair.Value == clazz)
                    return pair.Value;
            }

            _generatedTypes[key] = clazz;
            return clazz;
        }

        private FieldType GuessTokenTypeFromArray(string name, RavenJArray array)
        {
            var token = array.First();

            switch (token.Type)
            {
                case JTokenType.Object:
                    return GenerateClassTypesFromObject(name, (RavenJObject)token);
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
                case JTokenType.Boolean: return this.KnownTypes[typeof(bool)];
                case JTokenType.Bytes: return this.KnownTypes[typeof(byte[])];

                case JTokenType.Date: return this.KnownTypes[typeof(DateTimeOffset)];

                case JTokenType.Guid: return this.KnownTypes[typeof(Guid)];
                case JTokenType.TimeSpan: return this.KnownTypes[typeof(TimeSpan)];
                case JTokenType.Uri: return this.KnownTypes[typeof(Uri)];
                case JTokenType.Float: return this.KnownTypes[typeof(float)];

                case JTokenType.Integer: // Could be integer or long.                
                case JTokenType.String: // Could be anything.
                case JTokenType.Undefined:
                    return GuessTokenTypeFromContent(token);

                // Could be anything.
                case JTokenType.Null:
                    return this.KnownTypes[typeof(object)];

                default:
                    throw new NotSupportedException("We shouldn't have hit this. This is a bug in the caller routine.");
            }
        }

        private FieldType GuessTokenTypeFromContent(RavenJToken token)
        {
            var content = token.Value<string>();

            if (ParseHelper.TryAction<bool>(x => bool.TryParse(content, out x)))
                return KnownTypes[typeof(bool)];
            if (ParseHelper.TryAction<int>(x => int.TryParse(content, out x)))
                return KnownTypes[typeof(int)];
            if (ParseHelper.TryAction<long>(x => long.TryParse(content, out x)))
                return KnownTypes[typeof(long)];

            if (ParseHelper.TryAction<float>(x => float.TryParse(content, out x)))
                return KnownTypes[typeof(float)];
            if (ParseHelper.TryAction<double>(x => double.TryParse(content, out x)))
                return KnownTypes[typeof(double)];

            if (ParseHelper.TryAction<Guid>(x => Guid.TryParse(content, out x)))
                return KnownTypes[typeof(Guid)];
            if (ParseHelper.TryAction<TimeSpan>(x => TimeSpan.TryParse(content, out x)))
                return KnownTypes[typeof(TimeSpan)];
            if (ParseHelper.TryAction<DateTimeOffset>(x => DateTimeOffset.TryParse(content, out x)))
                return KnownTypes[typeof(DateTimeOffset)];

            
            if (Uri.IsWellFormedUriString(content, UriKind.Absolute))
                return KnownTypes[typeof(Uri)];

            return KnownTypes[typeof(string)];
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

namespace ##namespace
{
##code
}
";

    }
}
