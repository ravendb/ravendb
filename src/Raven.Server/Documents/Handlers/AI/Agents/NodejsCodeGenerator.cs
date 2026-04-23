using System;
using System.Collections;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class NodejsCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        return $$"""
                 const { DocumentStore } = require('ravendb');

                 async function runConversation() {
                 {{GenerateObject(obj, varName, indent: 1).TrimEnd()}}

                     const documentStore = new DocumentStore('http://localhost:8080', 'YourDatabase');
                     documentStore.initialize();

                     // Create/deploy the agent
                     const createdAgentResult = await documentStore.ai.createAgent({{varName}});

                     // Create a conversation/chat with the agent
                     const chat = documentStore.ai.conversation(
                         createdAgentResult.identifier,  // The agent ID
                         'Conversations/',                // The conversation document prefix
                         {{GenerateConversationParameters(obj)}}
                     );

                 {{GenerateHandleCalls(obj)}}
                     // Set user prompt
                     chat.setUserPrompt('Your question here');

                     // Run the chat/conversation
                     const response = await chat.run();

                     if (response.status === 'Done') {
                         const answer = response.answer;
                         console.log(answer);
                     }
                 }

                 runConversation();
                 """;
    }

    private static string GenerateConversationParameters(AiAgentConfiguration obj)
    {
        var sb = new StringBuilder();
        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("{");
            sb.AppendLine("            parameters: {");
            for (int i = 0; i < obj.Parameters.Count; i++)
            {
                var param = obj.Parameters[i];
                var comma = i < obj.Parameters.Count - 1 ? "," : "";
                sb.AppendLine($"                {ToCamelCase(param.Name)}: 'your-{param.Name}-here'{comma}  // {param.Description}");
            }
            sb.AppendLine("            }");
            sb.AppendLine("        }");
        }
        return sb.ToString();
    }

    private static string GenerateHandleCalls(AiAgentConfiguration obj)
    {
        var sb = new StringBuilder();
        foreach (var action in obj.Actions ?? [])
        {
            sb.AppendLine($$"""
                                // Define a handler for the '{{action.Name}}' action tool
                                chat.handle('{{action.Name}}', async (params) => {
                            {{GetJsonComment(GetSampleObject(action.ParametersSampleObject, action.ParametersSchema), "        ")}}
                                    // TODO: handle '{{action.Name}}' action
                                    return 'done';
                                });

                            """);
        }
        return sb.ToString();
    }

    protected override string TransformPropertyName(string name) => ToCamelCase(name);

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
            return $"'{EscapeSingleQuoted(s)}'";

        var escaped = TryPrettyPrintJson(s)
            .Replace("\\", "\\\\")
            .Replace("`", "\\`")
            .Replace("${", "\\${");

        var indented = string.Join(System.Environment.NewLine,
            escaped.Split(System.Environment.NewLine)
                   .Select((line, i) => i == 0 ? line : Indent(indent) + line));
        return $"`{indented}`";
    }

    protected override void WriteRootPrefix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.AppendLine($"{Indent(indent)}const {varName} =");

    protected override void WriteRootSuffix(StringBuilder sb, object obj, string varName, int indent) =>
        sb.Append(";");

    protected override void WriteObjectStart(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}{{");

    protected override void WriteObjectEnd(StringBuilder sb, object obj, int indent) =>
        sb.Append($"{Indent(indent)}}}");

    protected override void WriteSimpleProperty(StringBuilder sb, string name, object value, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}{name}: {FormatValue(value, indent)}{(isLast ? "" : ",")}");

    protected override void WriteComplexProperty(StringBuilder sb, string name, object value, int indent, bool isLast)
    {
        sb.Append($"{Indent(indent)}{name}: ");
        WriteObjectInitializer(sb, value, indent, addComma: !isLast, isStartInNewLine: false);
    }

    protected override void WriteEnumerableStart(StringBuilder sb, string name, IEnumerable value, int indent) =>
        sb.AppendLine($"{Indent(indent)}{name}: [");

    protected override void WriteEnumerableItem(StringBuilder sb, object item, int indent, bool isLast) =>
        WriteObjectInitializer(sb, item, indent + 1, addComma: !isLast);

    protected override void WriteEnumerableEnd(StringBuilder sb, string name, int indent, bool isLast) =>
        sb.AppendLine($"{Indent(indent)}]{(isLast ? "" : ",")}");

    private static string GetJsonComment(string json, string linePrefix)
    {
        var sb = new StringBuilder();
        if (TryGetJsonKeys(json).Count == 0)
            return string.Empty;
        foreach (var line in TryPrettyPrintJson(json).Split(Environment.NewLine))
            sb.AppendLine($"{linePrefix}// {line}");

        return sb.ToString();
    }

    private static string ToCamelCase(string str) =>
        string.IsNullOrEmpty(str) ? str : char.ToLower(str[0]) + str[1..];
}
