using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public abstract class AbstractCodeGenerator
{
    public virtual string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj") =>
        GenerateObject(obj, varName);

    protected string GenerateObject(object obj, string varName = "obj", int indent = 0)
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
    protected abstract string FormatString(string s, int indent);

    protected virtual string TransformPropertyName(string name) => name;

    protected virtual string FormatValue(object value, int indent) => value switch
    {
        null => "null",
        string s => FormatString(s, indent),
        bool b => b.ToString().ToLower(),
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected virtual void WriteRoot(StringBuilder sb, object obj, string varName, int indent)
    {
        WriteRootPrefix(sb, obj, varName, indent);
        WriteObjectInitializer(sb, obj, indent, appendLineAfter: false);
        WriteRootSuffix(sb, obj, varName, indent);
    }

    protected void WriteObjectInitializer(
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

    protected void WriteProperties(StringBuilder sb, object obj, int indent)
    {
        var props = new List<(string name, object value)>();
        foreach (var member in GetReadableMembers(obj.GetType()))
        {
            var name = member.Name;
            var value = GetMemberValue(member, obj);
            if (IsEmptyValue(value) == false)
                props.Add((name, value));
        }

        for (int i = 0; i < props.Count; i++)
            WriteProperty(sb, props[i].name, props[i].value, indent, isLast: i == props.Count - 1);
    }

    private void WriteProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
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

    private void WriteEnumerableProperty(StringBuilder sb, string name, IEnumerable enumerable, int indent, bool isLast)
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

    protected string Indent(int level) => new(' ', level * 4);

    protected string EscapeSingleQuoted(string s) => s.Replace("\\", "\\\\").Replace("'", "\\'");

    protected static bool HasEscaping(string s) =>
        s.Contains('"') || s.Contains('\\') || s.Contains('\n') || s.Contains('\r');

    protected static string TryPrettyPrintJson(string s)
    {
        if (string.IsNullOrWhiteSpace(s))
            return s;
        try
        {
            var json = System.Text.Json.JsonDocument.Parse(s);
            return System.Text.Json.JsonSerializer.Serialize(json.RootElement,
                new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                });
        }
        catch { return s; }
    }

    protected static List<string> TryGetJsonKeys(string json)
    {
        try
        {
            var doc = System.Text.Json.JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object)
                return doc.RootElement.EnumerateObject().Select(p => p.Name).ToList();
        }
        catch { }
        return new List<string>();
    }

    private IEnumerable<MemberInfo> GetReadableMembers(Type type)
    {
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .Cast<MemberInfo>();

        var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance)
            .Cast<MemberInfo>();

        return properties.Concat(fields);
    }

    private static object GetMemberValue(MemberInfo member, object obj)
    {
        if (member is PropertyInfo prop)
            return prop.GetValue(obj);
        if (member is FieldInfo field)
            return field.GetValue(obj);
        return null;
    }

    private bool HasAnyPrintableProperty(object obj)
    {
        if (obj == null)
            return false;

        foreach (var member in GetReadableMembers(obj.GetType()))
        {
            var val = GetMemberValue(member, obj);
            if (IsEmptyValue(val) == false)
                return true;
        }

        return false;
    }

    private bool IsEmptyValue(object value)
    {
        if (value == null)
            return true;

        if (value is string s)
            return string.IsNullOrWhiteSpace(s);

        if (value is IEnumerable e)
        {
            foreach (var item in e)
            {
                if (IsEmptyValue(item) == false)
                    return false;
            }
            return true;
        }

        if (IsSimpleType(value.GetType()))
            return IsDefaultValue(value);

        return HasAnyPrintableProperty(value) == false;
    }

    private static bool IsDefaultValue(object value)
    {
        if (value == null)
            return true;
        var type = value.GetType();
        return type.IsValueType && value.Equals(Activator.CreateInstance(type));
    }

    private static bool IsSimpleType(Type type) =>
        type.IsPrimitive || type.IsEnum ||
        type == typeof(string) || type == typeof(decimal) || type == typeof(Guid);
}
