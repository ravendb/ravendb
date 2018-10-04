using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public static class RewritersHelper
    {
        public static HashSet<Field> ExtractFields(AnonymousObjectCreationExpressionSyntax anonymousObjectCreationExpressionSyntax, bool retrieveOriginal = false)
        {
            var fields = new HashSet<Field>();
            for (var i = 0; i < anonymousObjectCreationExpressionSyntax.Initializers.Count; i++)
            {
                var initializer = anonymousObjectCreationExpressionSyntax.Initializers[i];
                string name;
                if (initializer.NameEquals != null && retrieveOriginal == false)
                {
                    name = initializer.NameEquals.Name.Identifier.Text;
                }
                else
                {
                    if (initializer.Expression is MemberAccessExpressionSyntax memberAccessExpressionSyntax)
                    {
                        fields.Add(ExtractField(memberAccessExpressionSyntax));
                        continue;
                    }

                    var identifierNameSyntax = initializer.Expression as IdentifierNameSyntax;

                    if (identifierNameSyntax == null)
                        throw new NotSupportedException($"Cannot extract field name from: {initializer}");

                    name = identifierNameSyntax.Identifier.Text;
                }

                fields.Add(new SimpleField(name));
            }

            return fields;
        }

        public static Field ExtractField(MemberAccessExpressionSyntax expression)
        {
            var name = expression.Name.Identifier.Text;
            var path = ExtractPath(expression);

            if (path == null || path.Length <= 1)
                return new SimpleField(name);

            return new NestedField(path[0], path.Skip(1).ToArray());
        }

        private static string[] ExtractPath(MemberAccessExpressionSyntax expression)
        {
            var path = expression.ToString().Split(".");

            return path
                .Skip(1)                // skipping variable name e.g. 'result'
                .ToArray();
        }
    }

    public abstract class Field
    {
        protected Field(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name.TrimStart('@');
        }

        public readonly string Name;

        protected bool Equals(Field other)
        {
            return string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj is Field objField)
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

    public class SimpleField : Field
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

    public class NestedField : Field
    {
        private IPropertyAccessor _accessor;

        private readonly string[] _path;

        private readonly Field _field;

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
            if (_accessor == null)
                _accessor = TypeConverter.GetPropertyAccessor(value);

            value = _accessor.GetValue(_field.Name, value);
            blittableValue = null;

            if (_field is SimpleField)
                blittableValue = TypeConverter.ToBlittableSupportedType(value);

            return _field.GetValue(value, blittableValue);
        }
    }
}
