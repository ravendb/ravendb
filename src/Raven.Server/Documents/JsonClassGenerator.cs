using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class JsonClassGenerator
    {
        private readonly Lazy<IDictionary<Type, FieldType>> _knownTypes = new Lazy<IDictionary<Type, FieldType>>(InitializeKnownTypes, true);

        internal IDictionary<Type, FieldType> KnownTypes => _knownTypes.Value;

        private readonly IDictionary<string, ClassType> _generatedTypes = new Dictionary<string, ClassType>();

        private static IDictionary<Type, FieldType> InitializeKnownTypes()
        {
            var types = new Dictionary<Type, FieldType>
            {
                [typeof(bool)] = new FieldType("bool", false, true),
                [typeof(long)] = new FieldType("long", false, true),
                [typeof(int)] = new FieldType("int", false, true),
                [typeof(string)] = new FieldType("string", false, true),
                [typeof(float)] = new FieldType("float", false, true),
                [typeof(double)] = new FieldType("double", false, true),
                [typeof(object)] = new FieldType("object", false, true),
                [typeof(byte[])] = new FieldType("byte", true, true),
                [typeof(int[])] = new FieldType("int", true, true),
                [typeof(Guid)] = new FieldType(typeof(Guid)),
                [typeof(DateTime)] = new FieldType(typeof(DateTimeOffset).Name, false, true),
                [typeof(DateTimeOffset)] = new FieldType(typeof(DateTimeOffset)),
                [typeof(TimeSpan)] = new FieldType(typeof(TimeSpan)),
                [typeof(Uri)] = new FieldType(typeof(Uri))
            };

            return types;
        }

        internal class FieldType
        {
            public readonly string Name;
            public readonly bool IsPrimitive;
            public readonly bool IsArray;

            public FieldType(Type type, bool isArray = false)
            {
                Name = type.Name;

                IsPrimitive = true;
                IsArray = isArray;
            }

            public FieldType(string type, bool isArray = false, bool isPrimitive = false)
            {
                Name = type;
                IsPrimitive = isPrimitive;
                IsArray = isArray;
            }

            public override bool Equals(object obj)
            {
                // if parameter cannot be cast to FieldType return false:
                var p = obj as FieldType;
                if (p == null)
                    return false;

                // return true if the fields match:
                return this == p;
            }

            public bool Equals(FieldType p)
            {
                // return true if the fields match:
                return this == p;
            }

            public override int GetHashCode()
            {
                unchecked // overflow is fine, just wrap
                {
                    var hash = 17;
                    // suitable nullity checks etc, of course :)
                    hash = hash * 23 + Name.GetHashCode();
                    hash = hash * 23 + IsPrimitive.GetHashCode();
                    hash = hash * 23 + IsArray.GetHashCode();
                    return hash;
                }
            }

            public static bool operator ==(FieldType a, FieldType b)
            {
                // if both are null, or both are same instance, return true.
                if (ReferenceEquals(a, b))
                {
                    return true;
                }

                // if one is null, but not both, return false.
                if (((object)a == null) || ((object)b == null))
                {
                    return false;
                }

                // return true if the fields match:
                return a.Name == b.Name && a.IsPrimitive == b.IsPrimitive && a.IsArray == b.IsArray;
            }

            public static bool operator !=(FieldType a, FieldType b)
            {
                return !(a == b);
            }
        }

        internal class ClassType : FieldType
        {
            public readonly IDictionary<string, FieldType> Properties;

            public ClassType(string name, IDictionary<string, FieldType> properties = null) : base(name)
            {
                Properties = new ReadOnlyDictionary<string, FieldType>(properties);
            }

            public static bool operator ==(ClassType a, ClassType b)
            {
                // if both are null, or both are same instance, return true.
                if (ReferenceEquals(a, b))
                    return true;

                // if one is null, but not both, return false.
                if (((object)a == null) || ((object)b == null))
                {
                    return false;
                }

                return Compare(a.Properties, b.Properties);
            }

            public static bool operator !=(ClassType a, ClassType b)
            {
                return !(a == b);
            }

            public override bool Equals(object obj)
            {
                // if parameter cannot be cast to FieldType return false:
                var p = obj as ClassType;
                if (p == null)
                    return false;

                // return true if the fields match:
                return this == p;
            }

            public override int GetHashCode()
            {
                unchecked // overflow is fine, just wrap
                {
                    var hash = 17;

                    // suitable nullity checks etc, of course :)
                    hash = hash * 23 + IsPrimitive.GetHashCode();
                    hash = hash * 23 + IsArray.GetHashCode();
                    hash = hash * 23 + Properties.GetHashCode();

                    return hash;
                }
            }

            private static bool Compare<TKey, TValue>(IDictionary<TKey, TValue> dict1, IDictionary<TKey, TValue> dict2)
            {
                if (dict1 == dict2)
                    return true;
                if ((dict1 == null) || (dict2 == null))
                    return false;
                if (dict1.Count != dict2.Count)
                    return false;

                var comparer = EqualityComparer<TValue>.Default;

                foreach (KeyValuePair<TKey, TValue> kvp in dict1)
                {
                    if (!dict2.TryGetValue(kvp.Key, out TValue value2))
                        return false;

                    if (!comparer.Equals(kvp.Value, value2))
                        return false;
                }

                return true;
            }
        }

        public JsonClassGenerator(string language)
        {
            if (string.IsNullOrWhiteSpace(language))
                throw new ArgumentNullException("language");
        }

        public string Execute(Document document)
        {
            if (document == null)
                throw new ArgumentNullException("document");

            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);

            var @class = "Class";
            var @namespace = "Unknown";
            if (metadata != null)
            {
                // retrieve the class and metadata if available.
                // "Raven-Clr-Type": "Namespace.ClassName, AssemblyName"

                if (metadata.TryGet(Constants.Documents.Metadata.RavenClrType, out LazyStringValue lazyStringValue))
                {
                    var values = lazyStringValue.ToString().Split(',');
                    if (values.Length == 2)
                    {
                        var data = values[0];
                        var index = data.LastIndexOf('.');
                        if (index > 0)
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

            var classes = GenerateClassesTypesFromObject(@class, document);

            var classCode = GenerateClassCodeFromSpec(classes);

            var code = CodeLayout.Replace("##namespace", @namespace)
                                 .Replace("##code", classCode);

            return code;
        }

        private static string GenerateClassCodeFromSpec(IReadOnlyList<ClassType> classes)
        {
            var codeBuilder = new StringBuilder();

            for (var i = 0; i < classes.Count; i++)
            {
                var @class = classes[i];
                codeBuilder.Append("\tpublic class " + @class.Name + Environment.NewLine);
                codeBuilder.Append("\t{" + Environment.NewLine);

                foreach (var field in @class.Properties)
                {
                    codeBuilder.Append("\t\tpublic ");
                    codeBuilder.Append(field.Value.IsArray ? $"List<{field.Value.Name}>" : field.Value.Name);
                    codeBuilder.Append(" ");
                    codeBuilder.Append(field.Key + " { get; set; } ");
                    codeBuilder.Append(Environment.NewLine);
                }

                codeBuilder.Append("\t}");

                if (i < classes.Count - 1)
                    codeBuilder.Append(Environment.NewLine + Environment.NewLine);
            }

            return codeBuilder.ToString();
        }

        internal List<ClassType> GenerateClassesTypesFromObject(string name, Document document)
        {
            // we need to clear the generated types;
            _generatedTypes.Clear();

            // repopulate the generated types after working on the object.
            var root = GenerateClassTypesFromObject(name, document.Data);

            foreach (var pair in _generatedTypes.ToList())
            {
                var @class = pair.Value;

                var changed = false;
                var properties = @class.Properties;
                foreach (var fieldPair in properties.ToList())
                {
                    var field = fieldPair.Value;
                    if (field.Name == root.Name)
                    {
                        properties[fieldPair.Key] = new FieldType(name, field.IsArray, field.IsPrimitive);
                        changed = true;
                    }
                }

                if (@class.Name == root.Name)
                {
                    _generatedTypes[pair.Key] = new ClassType(name, properties);
                }
                else if (changed)
                {
                    _generatedTypes[pair.Key] = new ClassType(@class.Name, properties);
                }
            }

            // return all the potential classes found.
            return _generatedTypes.Select(x => x.Value).ToList();
        }

        internal ClassType GenerateClassTypesFromObject(string name, BlittableJsonReaderObject blittableObject)
        {
            var fields = new Dictionary<string, FieldType>();

            for (var i = 0; i < blittableObject.Count; i++)
            {
                // this call ensures properties to be returned in the same order, regardless their storing order
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                blittableObject.GetPropertyByIndex(i, ref prop);

                if (prop.Name.ToString().Equals(Constants.Documents.Metadata.Key, StringComparison.OrdinalIgnoreCase))
                    continue;

                switch (prop.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.EmbeddedBlittable:
                    case BlittableJsonToken.StartObject:
                        var type = GenerateClassTypesFromObject(prop.Name, (BlittableJsonReaderObject)prop.Value);
                        fields[prop.Name] = new FieldType(type.Name, type.IsArray);
                        break;
                    case BlittableJsonToken.StartArray:
                        var array = (BlittableJsonReaderArray)prop.Value;
                        fields[prop.Name] = GetArrayField(array, prop.Name);
                        break;
                    default:
                        fields[prop.Name] = GetTokenTypeFromPrimitiveType(prop.Token, prop.Value);
                        break;
                }
            }

            // check if we can get the name from the metadata. 
            var clazz = new ClassType(name, fields);
            clazz = IncludeGeneratedClass(clazz);
            return clazz;
        }

        private FieldType GetArrayField(BlittableJsonReaderArray array, string name)
        {
            if (array.Length == 0)
            {
                return new FieldType("object", true, true);
            }

            var guessedType = GuessTokenTypeFromArray(name, array);
            if (guessedType is ClassType)
            {
                // we will defer the analysis and create a new name;
                return new FieldType(guessedType.Name, true);
            }

            return new FieldType(guessedType.Name, true, true);
        }

        private ClassType IncludeGeneratedClass(ClassType clazz)
        {
            var key = clazz.Name;

            var i = 1;
            while (_generatedTypes.TryGetValue(key, out ClassType dummy))
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

        private FieldType GuessTokenTypeFromArray(string name, BlittableJsonReaderArray array)
        {
            var firstElement = array.GetValueTokenTupleByIndex(0);

            switch (firstElement.Item2)
            {
                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                case BlittableJsonToken.StartObject | BlittableJsonToken.OffsetSizeByte | BlittableJsonToken.PropertyIdSizeByte:
                    return GenerateClassTypesFromObject(name, (BlittableJsonReaderObject)firstElement.Item1);
                case BlittableJsonToken.StartArray:
                case BlittableJsonToken.StartArray | BlittableJsonToken.OffsetSizeByte:
                case BlittableJsonToken.StartArray | BlittableJsonToken.OffsetSizeShort:
                    var type = GuessTokenTypeFromArray(name, (BlittableJsonReaderArray)firstElement.Item1);
                    return new FieldType($"List<{type.Name}>", true);
                default:
                    return GetTokenTypeFromPrimitiveType(firstElement.Item2, firstElement.Item1);
            }
        }

        private FieldType GetTokenTypeFromPrimitiveType(BlittableJsonToken token, object value)
        {
            switch (token)
            {
                // base types
                case BlittableJsonToken.Boolean:
                    return KnownTypes[typeof(bool)];
                case BlittableJsonToken.LazyNumber:
                    return KnownTypes[typeof(float)];
                case BlittableJsonToken.Null:
                    return KnownTypes[typeof(object)];
                case BlittableJsonToken.Integer: // could be integer or long               
                case BlittableJsonToken.String: // could be anything
                case BlittableJsonToken.CompressedString:
                    return GuessTokenTypeFromContent(value);
                default:
                    throw new NotSupportedException("We shouldn't have hit this. This is a bug in the caller routine.");
            }
        }

        private FieldType GuessTokenTypeFromContent(object value)
        {
            var content = value.ToString();

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
            public static bool TryAction<T>(Func<T, bool> action)
            {
                var t = default(T);
                return action(t);
            }
        }

        private const string CodeLayout =
@"using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ##namespace
{
##code
}";
    }
}
