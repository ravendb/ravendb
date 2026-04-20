using System;
using System.Collections;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class PythonCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        return $$"""
                 from ravendb import *

                 document_store = DocumentStore(
                     urls=["http://127.0.0.1:8080"],
                     database="YourDatabaseName"
                 )
                 document_store.initialize()

                 {{GenerateObject(obj, varName)}}

                 # Create/deploy the agent
                 agent_id = document_store.ai.add_or_update_agent({{varName}}).identifier

                 # Create a conversation/chat with the agent
                 {{GenerateConversation(obj)}}

                 {{GenerateHandleCalls(obj)}}
                 # Set user prompt and run
                 chat.set_user_prompt('Your question here')
                 result = chat.run()
                 answer = result.answer
                 """;
    }

    private static string GenerateConversation(AiAgentConfiguration obj)
    {
        var sb = new StringBuilder();
        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("with document_store.ai.conversation(");
            sb.AppendLine("    agent_id,");
            sb.AppendLine("    conversation_id='Conversations/',");
            sb.AppendLine("    parameters={");
            foreach (var param in obj.Parameters)
                sb.AppendLine($"        '{param.Name}': 'your-{param.Name}-here',  # {param.Description}");
            sb.AppendLine("    }");
            sb.AppendLine(") as chat:");
        }
        else
        {
            sb.AppendLine("with document_store.ai.conversation(agent_id, conversation_id='Conversations/') as chat:");
        }
        return sb.ToString();
    }

    private static string GenerateHandleCalls(AiAgentConfiguration obj)
    {
        var sb = new StringBuilder();
        foreach (var action in obj.Actions ?? [])
        {
            var handlerName = $"handle_{ToSnakeCase(action.Name)}";
            sb.AppendLine($"# Define a handler for the '{action.Name}' action tool");
            sb.AppendLine($"def {handlerName}(params):");
            AppendJsonComment(sb, action.ParametersSampleObject, "    ");
            sb.AppendLine($"    # TODO: handle '{action.Name}' action");
            sb.AppendLine($"    return 'done'");
            sb.AppendLine();
            sb.AppendLine($"chat.handle('{action.Name}', {handlerName})");
            sb.AppendLine();
        }
        return sb.ToString();
    }

    protected override string TransformPropertyName(string name) => ToSnakeCase(name);

    protected override string FormatValue(object value, int indent) => value switch
    {
        null => "None",
        string s => FormatString(s, indent),
        bool b => b ? "True" : "False",
        Enum e => $"{e.GetType().Name}.{e}",
        _ => value.ToString()
    };

    protected override string FormatString(string s, int indent)
    {
        if (HasEscaping(s) == false)
            return $"'{EscapeSingleQuoted(s)}'";

        var pretty = TryPrettyPrintJson(s);
        var indented = string.Join(System.Environment.NewLine,
            pretty.Split(System.Environment.NewLine)
                  .Select((line, i) => i == 0 ? line : Indent(indent) + line));
        return $"\"\"\"{indented}\"\"\"";
    }

    // Python uses constructor-call syntax — no braces, properties go directly inside "("
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

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) { }  // not used in Python

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.AppendLine($"{Indent(indent)})");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name}={FormatValue(value, indent)}{(isLast ? "" : ",")}");

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

    private static void AppendJsonComment(StringBuilder sb, string json, string linePrefix)
    {
        if (TryGetJsonKeys(json).Count == 0)
            return;
        foreach (var line in TryPrettyPrintJson(json).Split(System.Environment.NewLine))
            sb.AppendLine($"{linePrefix}# {line}");
    }

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
