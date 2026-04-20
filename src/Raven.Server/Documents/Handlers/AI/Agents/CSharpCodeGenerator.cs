using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
                     conversationId: "Conversations/",
                     {{GenerateConversationParameters(obj)}}
                 );

                 {{GenerateHandleCalls(obj)}}

                 // Set user prompt and run
                 conversation.SetUserPrompt("Your question here");

                 {{GenerateResponseClassDefinition(obj)}}var result = await conversation.RunAsync<{{GetResponseTypeName(obj)}}>();
                 var answer = result.Answer;
                 """;
    }

    private static string GenerateConversationParameters(AiAgentConfiguration obj)
    {
        var sb = new StringBuilder();
        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("new AiConversationCreationOptions()");
            foreach (var param in obj.Parameters)
                sb.AppendLine($"        .AddParameter(\"{param.Name}\", \"your-{param.Name}-here\")  // {param.Description}");
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
            var argsClassName = $"{action.Name}Args";
            var argsClassDef = GenerateArgsClass(argsClassName, action.ParametersSampleObject);
            if (argsClassDef != null)
            {
                sb.AppendLine(argsClassDef);
                sb.AppendLine();
            }

            var argType = argsClassDef != null ? argsClassName : "object";
            sb.AppendLine($"// Define a handler for the \"{action.Name}\" action tool");
            sb.AppendLine($"conversation.Handle<{argType}, ActionToolResult>(\"{action.Name}\", async (args) =>");
            sb.AppendLine("{");
            sb.AppendLine($"    // TODO: handle \"{action.Name}\" action");
            sb.AppendLine("    return new ActionToolResult { IsSuccessful = true };");
            sb.AppendLine("});");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static string GenerateArgsClass(string className, string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;
        try
        {
            var doc = System.Text.Json.JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Object)
                return null;
            var props = doc.RootElement.EnumerateObject().ToList();
            if (props.Count == 0)
                return null;

            var sb = new StringBuilder();
            sb.AppendLine($"class {className}");
            sb.AppendLine("{");
            foreach (var prop in props)
                sb.AppendLine($"    public {GetCSharpType(prop.Value)} {prop.Name} {{ get; set; }}");
            sb.Append("}");
            return sb.ToString();
        }
        catch { return null; }
    }

    private static string GenerateResponseClassDefinition(AiAgentConfiguration obj)
    {
        if (string.IsNullOrWhiteSpace(obj.SampleObject))
            return string.Empty;
        try
        {
            var doc = System.Text.Json.JsonDocument.Parse(obj.SampleObject);
            if (doc.RootElement.ValueKind != System.Text.Json.JsonValueKind.Object)
                return string.Empty;
            var props = doc.RootElement.EnumerateObject().ToList();
            if (props.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();
            sb.AppendLine("class AgentResponse");
            sb.AppendLine("{");
            foreach (var prop in props)
                sb.AppendLine($"    public {GetCSharpType(prop.Value)} {prop.Name} {{ get; set; }}");
            sb.AppendLine("}");
            sb.AppendLine();
            return sb.ToString();
        }
        catch { return string.Empty; }
    }

    private static string GetResponseTypeName(AiAgentConfiguration obj) =>
        string.IsNullOrWhiteSpace(obj.SampleObject) ? "/* your response type */" : "AgentResponse";

    private static string GetCSharpType(System.Text.Json.JsonElement element) => element.ValueKind switch
    {
        System.Text.Json.JsonValueKind.String => "string",
        System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out _) ? "long" : "double",
        System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => "bool",
        System.Text.Json.JsonValueKind.Array => $"List<{GetArrayItemType(element)}>",
        _ => "object"
    };

    private static string GetArrayItemType(System.Text.Json.JsonElement array)
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
        if (HasEscaping(s) == false)
            return $"\"{s}\"";

        var pretty = TryPrettyPrintJson(s);
        if (pretty.StartsWith("{"))
            pretty = Environment.NewLine + pretty;
        if (pretty.EndsWith("}"))
            pretty += Environment.NewLine;

        var indented = string.Join(Environment.NewLine,
            pretty.Split(Environment.NewLine)
                .Select((line, i) => i == 0 ? line : Indent(indent) + line));

        return $"\"\"\"{indented}\"\"\"";
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
        sb.AppendLine($"{Indent(indent)}{name} = new {GetFriendlyTypeName(value.GetType())}\n{Indent(indent)}{{");

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

    private static void AppendJsonComment(StringBuilder sb, string json, string linePrefix)
    {
        if (TryGetJsonKeys(json).Count == 0)
            return;
        foreach (var line in TryPrettyPrintJson(json).Split(Environment.NewLine))
            sb.AppendLine($"{linePrefix}// {line}");
    }
}
