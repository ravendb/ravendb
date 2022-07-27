using System;
using System.Linq;
using System.Text;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Indexes
{
    public abstract class CompiledIndexField
    {
        protected CompiledIndexField(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name.TrimStart('@');
        }

        public readonly string Name;

        protected virtual bool Equals(CompiledIndexField other)
        {
            return string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj is CompiledIndexField objField)
                return Equals(objField);
            return false;
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public bool IsMatch(string name)
        {
            return string.Equals(Name, name);
        }

        public abstract void WriteTo(StringBuilder sb);

        public abstract object GetValue(object value, object blittableValue);
    }

    public class SimpleField : CompiledIndexField
    {
        public SimpleField(string name)
            : base(name)
        {
        }

        public override void WriteTo(StringBuilder sb)
        {
            sb
                .Append("new ")
                .Append(typeof(SimpleField).FullName)
                .Append("(\"")
                .Append(Name)
                .Append("\")");
        }

        public override object GetValue(object value, object blittableValue)
        {
            return blittableValue;
        }
    }

    public class JsNestedField : NestedField
    {
        public readonly string PropertyName;

        public JsNestedField(string propertyName, string name, string[] path)
            : base(name, path)
        {
            PropertyName = propertyName;
        }

        protected override bool Equals(CompiledIndexField other)
        {
            if (other is JsNestedField jnf)
            {
                if (PropertyName != jnf.PropertyName)
                    return false;
                return base.Equals(other);
            }

            return false;
        }

        public override void WriteTo(StringBuilder sb)
        {
            throw new NotSupportedException();
        }

        public override object GetValue(object value, object blittableValue)
        {
            if (blittableValue is BlittableJsonReaderObject bjro)
            {
                if (bjro.TryGet(_field.Name, out blittableValue))
                    return _field.GetValue(null, blittableValue);
            }

            throw new NotSupportedException($"Was expecting JSON object but got '{blittableValue?.GetType().Name}'");
        }
    }

    public class NestedField : CompiledIndexField
    {
        private Type _accessorType;

        private IPropertyAccessor _accessor;

        private readonly string[] _path;

        protected readonly CompiledIndexField _field;

        public NestedField(string name, string[] path)
            : base(name)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            if (path.Length == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(path));

            _path = path;

            if (path.Length == 1)
                _field = new SimpleField(path[0]);
            else
                _field = new NestedField(path[0], path.Skip(1).ToArray());
        }

        protected override bool Equals(CompiledIndexField other)
        {
            if (other is NestedField nf)
            {
                if (_path.Length != nf._path.Length)
                    return false;
                for (int i = 0; i < _path.Length; i++)
                {
                    if (_path[i] != nf._path[i])
                        return false;
                }
                return base.Equals(other);
            }

            return false;
        }

        public override void WriteTo(StringBuilder sb)
        {
            sb
                .Append("new ")
                .Append(typeof(NestedField).FullName)
                .Append("(\"")
                .Append(Name)
                .Append("\", new [] {");

            for (var i = 0; i < _path.Length; i++)
            {
                if (i > 0)
                    sb.Append(", ");

                sb
                    .Append("\"")
                    .Append(_path[i])
                    .Append("\"");
            }

            sb.Append("})");
        }

        public override object GetValue(object value, object blittableValue)
        {
            var valueType = value?.GetType();
            if (_accessorType != valueType)
                _accessor = null;

            if (_accessor == null)
            {
                _accessor = TypeConverter.GetPropertyAccessor(value);
                _accessorType = valueType;
            }

            value = _accessor.GetValue(_field.Name, value);
            blittableValue = null;

            if (_field is SimpleField)
                blittableValue = TypeConverter.ToBlittableSupportedType(value);

            return _field.GetValue(value, blittableValue);
        }
    }
}
