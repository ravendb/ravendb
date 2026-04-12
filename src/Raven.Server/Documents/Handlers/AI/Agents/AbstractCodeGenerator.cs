using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public abstract class AbstractCodeGenerator
{
    public string Generate(object obj, string varName = "obj", int indent = 0)
    {
        var sb = new StringBuilder();
        WriteRoot(sb, obj, varName, indent);
        return sb.ToString();
    }

    protected abstract void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent);
    protected abstract void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent);
    protected abstract void WriteObjectStart(StringBuilder sb, object obj, int indent);
    protected abstract void WriteObjectEnd(StringBuilder sb, object obj, int indent);
    protected abstract void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast);
    protected abstract void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast);
    protected abstract void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent);
    protected abstract void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast);
    protected abstract void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast);

    protected virtual string TransformPropertyName(string name) => name;

    protected virtual void WriteRoot(StringBuilder sb, object obj, string varName, int indent)
    {
        WriteRootPrefix(sb, obj, varName, indent);
        WriteObjectInitializer(sb, obj, indent, appendLineAfter: false);
        WriteRootSuffix(sb, obj, varName, indent);
    }

    protected virtual void WriteObjectInitializer(
        StringBuilder sb, object obj, int indent,
        bool addComma = false, bool appendLineAfter = true, bool isStartInNewLine = true)
    {
        WriteObjectStart(sb, obj, isStartInNewLine ? indent : 0);
        sb.AppendLine();

        WriteProperties(sb, obj, indent + 1);

        WriteObjectEnd(sb, obj, indent);
        if (addComma)
            sb.Append(",");
        if (appendLineAfter)
            sb.AppendLine();
    }

    /// <summary>
    /// Collects all printable properties, then writes each one passing isLast=true
    /// only to the final entry — so subclasses know whether to append a trailing comma.
    /// </summary>
    protected void WriteProperties(StringBuilder sb, object obj, int indent)
    {
        var props = GetReadableProperties(obj.GetType())
            .Select(p => (prop: p, value: p.GetValue(obj)))
            .Where(x => IsEmptyValue(x.value) == false)
            .ToList();

        for (int i = 0; i < props.Count; i++)
            WriteProperty(sb, props[i].prop.Name, props[i].value, indent, isLast: i == props.Count - 1);
    }

    protected virtual void WriteProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        var transformedName = TransformPropertyName(name);

        if (IsSimpleType(value.GetType()))
        {
            if (IsDefaultValue(value) == false)
                WriteSimpleProperty(sb, transformedName, value, indent, isLast);
            return;
        }

        if (value is IEnumerable enumerable)
        {
            WriteEnumerableProperty(sb, transformedName, enumerable, indent, isLast);
            return;
        }

        if (HasAnyPrintableProperty(value))
            WriteComplexProperty(sb, transformedName, value, indent, isLast);
    }

    protected virtual void WriteEnumerableProperty(StringBuilder sb, string name, IEnumerable enumerable, int indent, bool isLast)
    {
        var items = enumerable.Cast<object>()
            .Where(x => IsEmptyValue(x) == false && HasAnyPrintableProperty(x))
            .ToList();

        if (items.Count == 0)
            return;

        WriteEnumerableStart(sb, name, enumerable, indent);
        for (int i = 0; i < items.Count; i++)
            WriteEnumerableItem(sb, items[i], indent, isLast: i == items.Count - 1);
        WriteEnumerableEnd(sb, name, indent, isLast);
    }

    // Reflection helpers:

    protected IEnumerable<PropertyInfo> GetReadableProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead);

    protected bool HasAnyPrintableProperty(object obj) =>
        obj != null &&
        GetReadableProperties(obj.GetType()).Any(p => IsEmptyValue(p.GetValue(obj)) == false);

    protected bool IsEmptyValue(object value) => value switch
    {
        null => true,
        string s => string.IsNullOrWhiteSpace(s),
        IEnumerable e => e.Cast<object>().All(IsEmptyValue),
        _ when IsSimpleType(value.GetType()) => IsDefaultValue(value),
        _ => HasAnyPrintableProperty(value) == false
    };

    protected static bool IsDefaultValue(object value)
    {
        if (value == null)
            return true;
        var type = value.GetType();
        return type.IsValueType && value.Equals(Activator.CreateInstance(type));
    }

    protected bool IsSimpleType(Type type) =>
        type.IsPrimitive || type.IsEnum ||
        type == typeof(string) || type == typeof(decimal) || type == typeof(Guid);

    protected virtual string FormatValue(object value) => value switch
    {
        null => "null",
        string s => $"\"{EscapeDoubleQuoted(s)}\"",
        bool b => b.ToString().ToLower(),
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected string EscapeDoubleQuoted(string s) =>
        s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    protected string EscapeSingleQuoted(string s) =>
        s.Replace("\\", "\\\\").Replace("'", "\\'");

    protected string Indent(int level) => new(' ', level * 4);

    protected string GetFriendlyTypeName(Type type)
    {
        if (type.IsGenericType == false)
            return type.Name;
        var baseName = type.GetGenericTypeDefinition().Name[..type.GetGenericTypeDefinition().Name.IndexOf('`')];
        var args = string.Join(", ", type.GetGenericArguments().Select(t => t.Name));
        return $"{baseName}<{args}>";
    }
}

public class CSharpCodeGenerator : AbstractCodeGenerator
{
    protected override void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.AppendLine($"{Indent(indent)}var {varName} = new {obj.GetType().Name}");

    protected override void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.Append(";");

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}{{");

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}}}");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name} = {FormatValue(value)}{(isLast ? "" : ",")}");

    protected override void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent)}{name} = new {value.GetType().Name}");
        WriteObjectInitializer(sb, value, indent, addComma: isLast == false);
    }

    protected override void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent) =>
        sb.AppendLine($"{Indent(indent)}{name} = new {GetFriendlyTypeName(value.GetType())}\n{Indent(indent)}{{");

    protected override void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent + 1)}new {item.GetType().Name}");
        WriteObjectInitializer(sb, item, indent + 1, addComma: isLast == false);
    }

    protected override void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}}}{(isLast ? "" : ",")}");
}


public class NodejsCodeGenerator : AbstractCodeGenerator
{
    protected override string TransformPropertyName(string name) => ToCamelCase(name);
    protected override string FormatValue(object value) => value switch
    {
        null => "null",
        string s => $"'{EscapeSingleQuoted(s)}'",
        bool b => b.ToString().ToLower(),
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected override void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.AppendLine($"{Indent(indent)}const {varName} =");

    protected override void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.Append(";");

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}{{");

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}}}");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name}: {FormatValue(value)}{(isLast ? "" : ",")}");

    protected override void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        sb.Append($"{Indent(indent)}{name}: ");
        WriteObjectInitializer(sb, value, indent, addComma: isLast == false, isStartInNewLine: false);
    }

    protected override void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent) =>
        sb.AppendLine($"{Indent(indent)}{name}: [");

    protected override void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast) =>
        WriteObjectInitializer(sb, item, indent + 1, addComma: isLast == false);

    protected override void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}]{(isLast ? "" : ",")}");

    private static string ToCamelCase(string str) =>
        string.IsNullOrEmpty(str) ? str : char.ToLower(str[0]) + str[1..];
}


public class PythonCodeGenerator : AbstractCodeGenerator
{
    protected override string TransformPropertyName(string name) => ToSnakeCase(name);

    // Python strings use single quotes inside the value need no escaping
    protected override string FormatValue(object value) => value switch
    {
        null => "None",
        string s => $"'{EscapeSingleQuoted(s)}'",
        bool b => b ? "True" : "False",
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) { }

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.AppendLine($"{Indent(indent)})");

    // WriteRoot is overridden because Python's root uses "(" not "{" and there
    // is no brace-body — properties go directly inside the constructor call.
    protected override void WriteRoot(StringBuilder sb, object obj, string varName, int indent)
    {
        WriteRootPrefix(sb, obj, varName, indent);
        WriteProperties(sb, obj, indent + 1);
        WriteRootSuffix(sb, obj, varName, indent);
    }

    protected override void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.AppendLine($"{Indent(indent)}{varName} = {obj.GetType().Name}(");

    protected override void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.Append($"{Indent(indent)})");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name}={FormatValue(value)}{(isLast ? "" : ",")}");

    protected override void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent)}{name}={value.GetType().Name}(");
        WriteProperties(sb, value, indent + 1);
        sb.AppendLine($"{Indent(indent)}){(isLast ? "" : ",")}");
    }

    protected override void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent) =>
        sb.AppendLine($"{Indent(indent)}{name}=[");

    protected override void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent + 1)}{item.GetType().Name}(");
        WriteProperties(sb, item, indent + 2);
        sb.AppendLine($"{Indent(indent + 1)}){(isLast ? "" : ",")}");
    }

    protected override void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}]{(isLast ? "" : ",")}");

    private static string ToSnakeCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;
        var sb = new StringBuilder();
        sb.Append(char.ToLower(str[0]));
        foreach (char c in str[1..])
            sb.Append(char.IsUpper(c) ? $"_{char.ToLower(c)}" : c.ToString());
        return sb.ToString();
    }
}

