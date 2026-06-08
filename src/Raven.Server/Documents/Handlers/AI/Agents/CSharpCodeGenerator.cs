using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class CSharpCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        return $$"""
                 using System;
                 using System.Collections.Generic;
                 using System.Threading.Tasks;
                 using Raven.Client.Documents;
                 using Raven.Client.Documents.AI;
                 using Raven.Client.Documents.Operations.AI.Agents;

                 {{GenerateObject(obj, varName)}}

                 var documentStore = new DocumentStore
                 {
                     Urls = new[] { "http://localhost:8080" },
                     Database = "TestDB"
                 }.Initialize();

                 // Create/deploy the agent
                 await documentStore.AI.CreateAgentAsync({{varName}});

                 // Create a conversation/chat with the agent
                 var conversation = documentStore.AI.Conversation(
                     agentId: "{{obj.Identifier}}",
                     conversationId: "Conversations/"{{GenerateConversationParameters(obj)}}
                 );

                 {{GenerateHandleCalls(obj)}}

                 // Set user prompt and run
                 conversation.SetUserPrompt("Your question here");

                 {{GenerateClassDefinition("AgentResponse", obj.SampleObject, obj.OutputSchema)}}
                 var result = await conversation.RunAsync<AgentResponse>();
                 var answer = result.Answer;
                 """;
    }

    private static string GenerateConversationParameters(AiAgentConfiguration obj)
    {
        // The comma separates conversationId from the options object.
        // It is emitted here (not in the template) so that when there are no parameters
        // the method call does not end with a trailing comma before the closing ')'.
        var sb = new StringBuilder();
        sb.Append(",");
        sb.AppendLine();
        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("    new AiConversationCreationOptions()");
            foreach (var param in obj.Parameters)
                sb.AppendLine($"        .AddParameter(\"{param.Name}\", \"your-{param.Name}-here\")  // {param.Description}");
        }
        else
        {
            // AiConversationCreationOptions is required even when no custom parameters are needed.
            sb.Append("    new AiConversationCreationOptions()");
        }
        return sb.ToString();
    }

    private static string GenerateHandleCalls(AiAgentConfiguration obj)
    {
        var actions = obj.Actions ?? [];
        if (actions.Count == 0)
            return string.Empty;

        var sb = new StringBuilder();
        sb.Append("""
        class ActionToolResult
        {
            public bool IsSuccessful { get; set; }
            public string Answer { get; set; }
        }

        """);

        foreach (var action in actions)
        {
            var safeActionName = ToValidCSharpIdentifier(action.Name);
            var argsClassName = $"{safeActionName}Args";
            var argsClassDef = GenerateClassDefinition(argsClassName, action.ParametersSampleObject, action.ParametersSchema);
            if (argsClassDef != null)
            {
                sb.AppendLine(argsClassDef);
                sb.AppendLine();
            }

            var argType = argsClassDef != null ? argsClassName : "object";
            sb.AppendLine($$"""
                            // Define a handler for the "{{action.Name}}" action tool
                            conversation.Handle<{{argType}}, ActionToolResult>("{{action.Name}}", async (args) =>
                            {
                                // TODO: handle "{{action.Name}}" action
                                return new ActionToolResult { IsSuccessful = true };
                            });

                            """);
        }
        return sb.ToString();
    }

    private static string GenerateClassDefinition(string className, string sampleObject, string outputSchema)
    {
        var json = GetSampleObject(sampleObject, outputSchema);
        if (string.IsNullOrWhiteSpace(json))
            return null;
        try
        {
            var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return null;
            var props = doc.RootElement.EnumerateObject().ToList();
            if (props.Count == 0)
                return null;

            var sb = new StringBuilder();
            sb.AppendLine($"class {className}");
            sb.AppendLine("{");
            foreach (var prop in props)
            {
                var safeName = ToValidCSharpIdentifier(prop.Name);
                if (safeName != prop.Name)
                    sb.AppendLine($"    [System.Text.Json.Serialization.JsonPropertyName(\"{prop.Name}\")]");
                sb.AppendLine($"    public {GetCSharpType(prop.Value)} {safeName} {{ get; set; }}");
            }
            sb.Append("}");
            return sb.ToString();
        }
        catch (JsonException) { return null; }
    }

    private static string GetCSharpType(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => "string",
        JsonValueKind.Number => element.TryGetInt64(out _) ? "long" : "double",
        JsonValueKind.True or JsonValueKind.False => "bool",
        JsonValueKind.Array => $"List<{GetArrayItemType(element)}>",
        _ => "object"
    };

    private static string GetArrayItemType(JsonElement array)
    {
        foreach (var item in array.EnumerateArray())
            return GetCSharpType(item);
        return "object";
    }

    protected override string FormatValue(object value, int indent) => value switch
    {
        null => "null",
        string s => FormatString(s, indent),
        bool b => b.ToString().ToLower(),
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected override string FormatString(string s, int indent)
    {
        var pretty = TryPrettyPrintJson(s);
        if (pretty == "{}")
            return $"\"{pretty}\"";

        // Normalize to \n for consistent cross-platform splitting.
        pretty = pretty.Replace("\r\n", "\n").Replace("\r", "\n");

        bool isMultiline = pretty.Contains('\n');

        // C# 11 multiline raw strings require the opening """ to be immediately followed
        // by a newline (no content on the opening line). Ensure this by prepending \n
        // whenever the content is either a JSON object or spans multiple lines.
        if (pretty.StartsWith("{") || isMultiline)
            pretty = "\n" + pretty;
        if (pretty.EndsWith("}") || isMultiline)
            pretty += "\n";

        var indented = string.Join("\n",
            pretty.Split('\n')
                .Select((line, i) => i == 0 ? line : Indent(indent) + line));

        var fenceLength = GetStringQuotationFenceLength(indented);
        var fence = new string('"', fenceLength);
        return $"{fence}{indented}{fence}";
    }

    protected override void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.AppendLine($"{Indent(indent)}var {varName} = new {obj.GetType().Name}");

    protected override void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.Append(";");

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}{{");

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}}}");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name} = {FormatValue(value, indent)}{(isLast ? "" : ",")}");

    protected override void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent)}{name} = new {value.GetType().Name}");
        WriteObjectInitializer(sb, value, indent, addComma: !isLast);
    }

    protected override void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent) =>
        sb.AppendLine($"{Indent(indent)}{name} = new {GetFriendlyTypeName(value.GetType())}{Environment.NewLine}{Indent(indent)}{{");

    private string GetFriendlyTypeName(Type type)
    {
        if (type.IsGenericType == false)
            return type.Name;
        var baseName = type.GetGenericTypeDefinition().Name[..type.GetGenericTypeDefinition().Name.IndexOf('`')];
        var args = string.Join(", ", type.GetGenericArguments().Select(t => t.Name));
        return $"{baseName}<{args}>";
    }

    protected override void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast)
    {
        sb.AppendLine($"{Indent(indent + 1)}new {item.GetType().Name}");
        WriteObjectInitializer(sb, item, indent + 1, addComma: !isLast);
    }

    protected override void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}}}{(isLast ? "" : ",")}");

    // Replace every character that is not [a-zA-Z0-9_] with '_', and prefix '@' when
    // the result clashes with a C# keyword.
    private static string ToValidCSharpIdentifier(string name)
    {
        if (string.IsNullOrEmpty(name))
            return "_";

        var sb = new StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            bool valid = i == 0 ? (char.IsLetter(c) || c == '_') : (char.IsLetterOrDigit(c) || c == '_');
            sb.Append(valid ? c : '_');
        }

        // If first char was a digit it was replaced with '_', so no digit-prefix check needed.
        var result = sb.ToString();
        return CSharpKeywords.Contains(result) ? "@" + result : result;
    }

    private static readonly HashSet<string> CSharpKeywords = new(StringComparer.Ordinal)
    {
        "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked",
        "class", "const", "continue", "decimal", "default", "delegate", "do", "double", "else",
        "enum", "event", "explicit", "extern", "false", "finally", "fixed", "float", "for",
        "foreach", "goto", "if", "implicit", "in", "int", "interface", "internal", "is", "lock",
        "long", "namespace", "new", "null", "object", "operator", "out", "override", "params",
        "private", "protected", "public", "readonly", "ref", "return", "sbyte", "sealed", "short",
        "sizeof", "stackalloc", "static", "string", "struct", "switch", "this", "throw", "true",
        "try", "typeof", "uint", "ulong", "unchecked", "unsafe", "ushort", "using", "virtual",
        "void", "volatile", "while"
    };
}
