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
        var props = GetReadableProperties(obj.GetType())
            .Select(p => (prop: p, value: p.GetValue(obj)))
            .Where(x => IsEmptyValue(x.value) == false)
            .ToList();

        for (int i = 0; i < props.Count; i++)
            WriteProperty(sb, props[i].prop.Name, props[i].value, indent, isLast: i == props.Count - 1);
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

    private IEnumerable<PropertyInfo> GetReadableProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanRead);

    private bool HasAnyPrintableProperty(object obj) =>
        obj != null && GetReadableProperties(obj.GetType()).Any(p => IsEmptyValue(p.GetValue(obj)) == false);

    private bool IsEmptyValue(object value) => value switch
    {
        null => true,
        string s => string.IsNullOrWhiteSpace(s),
        IEnumerable e => e.Cast<object>().All(IsEmptyValue),
        _ when IsSimpleType(value.GetType()) => IsDefaultValue(value),
        _ => HasAnyPrintableProperty(value) == false
    };

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


public class CSharpCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        var sb = new StringBuilder();

        sb.AppendLine("using System;");
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine("using System.Threading.Tasks;");
        sb.AppendLine("using Raven.Client.Documents;");
        sb.AppendLine("using Raven.Client.Documents.AI;");
        sb.AppendLine("using Raven.Client.Documents.Operations.AI.Agents;");
        sb.AppendLine();
        sb.AppendLine(GenerateObject(obj, varName));
        sb.AppendLine();

        sb.AppendLine("var documentStore = new DocumentStore");
        sb.AppendLine("{");
        sb.AppendLine("    Urls = new[] { \"http://localhost:8080\" },");
        sb.AppendLine("    Database = \"TestDB\"");
        sb.AppendLine("}.Initialize();");

        sb.AppendLine("// Create/deploy the agent");
        sb.AppendLine($"await documentStore.AI.CreateAgentAsync({varName});");
        sb.AppendLine();

        sb.AppendLine("// Create a conversation/chat with the agent");
        sb.AppendLine("var conversation = documentStore.AI.Conversation(");
        sb.AppendLine($"    agentId: \"{obj.Identifier}\",");
        sb.AppendLine("    conversationId: \"Conversations/\",");

        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("    new AiConversationCreationOptions()");
            foreach (var param in obj.Parameters)
                sb.AppendLine($"        .AddParameter(\"{param.Name}\", \"your-{param.Name}-here\")  // {param.Description}");
        }

        sb.AppendLine(");");
        sb.AppendLine();

        foreach (var action in obj.Actions ?? [])
        {
            sb.AppendLine($"// Define a handler for the \"{action.Name}\" action tool");
            sb.AppendLine($"conversation.Handle<object, string>(\"{action.Name}\", async (args) =>");
            sb.AppendLine("{");
            AppendJsonComment(sb, action.ParametersSampleObject, "    ");
            sb.AppendLine($"    // TODO: handle \"{action.Name}\" action");
            sb.AppendLine("    return \"done\";");
            sb.AppendLine("});");
            sb.AppendLine();
        }

        sb.AppendLine("// Set user prompt and run");
        sb.AppendLine("conversation.SetUserPrompt(\"Your question here\");");
        sb.AppendLine();
        sb.AppendLine("var result = await conversation.RunAsync</* your response type */>();");
        sb.AppendLine("var answer = result.Answer;");

        return sb.ToString();
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


public class NodejsCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        var sb = new StringBuilder();

        sb.AppendLine("const { DocumentStore } = require('ravendb');");
        sb.AppendLine();
        sb.AppendLine("async function runConversation() {");
        sb.AppendLine(GenerateObject(obj, varName, indent: 1).TrimEnd());
        sb.AppendLine();
        sb.AppendLine("    const documentStore = new DocumentStore('http://localhost:8080', 'YourDatabase');");
        sb.AppendLine("    documentStore.initialize();");
        sb.AppendLine();

        sb.AppendLine("    // Create/deploy the agent");
        sb.AppendLine($"    const createdAgentResult = await documentStore.ai.createAgent({varName});");
        sb.AppendLine();

        sb.AppendLine("    // Create a conversation/chat with the agent");
        sb.AppendLine("    const chat = documentStore.ai.conversation(");
        sb.AppendLine("        createdAgentResult.identifier,  // The agent ID");
        sb.AppendLine("        'Conversations/',               // The conversation document prefix");

        if (obj.Parameters is { Count: > 0 })
        {
            sb.AppendLine("        {");
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

        sb.AppendLine("    );");
        sb.AppendLine();

        foreach (var action in obj.Actions ?? [])
        {
            sb.AppendLine($"    // Define a handler for the '{action.Name}' action tool");
            sb.AppendLine($"    chat.handle('{action.Name}', async (params) => {{");
            AppendJsonComment(sb, action.ParametersSampleObject, "        ");
            sb.AppendLine($"        // TODO: handle '{action.Name}' action");
            sb.AppendLine("        return 'done';");
            sb.AppendLine("    });");
            sb.AppendLine();
        }

        sb.AppendLine("    // Set user prompt");
        sb.AppendLine("    chat.setUserPrompt('Your question here');");
        sb.AppendLine();
        sb.AppendLine("    // Run the chat/conversation");
        sb.AppendLine("    const response = await chat.run();");
        sb.AppendLine();
        sb.AppendLine("    if (response.status === 'Done') {");
        sb.AppendLine("        const answer = response.answer;");
        sb.AppendLine("        console.log(answer);");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        sb.AppendLine();
        sb.AppendLine("runConversation();");

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

        var indented = string.Join(Environment.NewLine,
            escaped.Split(Environment.NewLine)
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

    private static void AppendJsonComment(StringBuilder sb, string json, string linePrefix)
    {
        if (TryGetJsonKeys(json).Count == 0)
            return;
        foreach (var line in TryPrettyPrintJson(json).Split(Environment.NewLine))
            sb.AppendLine($"{linePrefix}// {line}");
    }

    private static string ToCamelCase(string str) =>
        string.IsNullOrEmpty(str) ? str : char.ToLower(str[0]) + str[1..];
}

public class PythonCodeGenerator : AbstractCodeGenerator
{
    public override string GenerateFullFile(AiAgentConfiguration obj, string varName = "obj")
    {
        var sb = new StringBuilder();

        sb.AppendLine("from ravendb import *");
        sb.AppendLine();
        sb.AppendLine("document_store = DocumentStore(");
        sb.AppendLine("    urls=[\"http://127.0.0.1:8080\"],");
        sb.AppendLine("    database=\"YourDatabaseName\"");
        sb.AppendLine(")");
        sb.AppendLine("document_store.initialize()").AppendLine();
        sb.AppendLine();

        sb.AppendLine(GenerateObject(obj, varName));
        sb.AppendLine();
        sb.AppendLine();

        sb.AppendLine("# Create/deploy the agent");
        sb.AppendLine($"agent_id = document_store.ai.add_or_update_agent({varName}).identifier");

        sb.AppendLine("# Create a conversation/chat with the agent");
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

        sb.AppendLine();

        foreach (var action in obj.Actions ?? [])
        {
            var handlerName = $"handle_{ToSnakeCase(action.Name)}";
            sb.AppendLine($"    # Define a handler for the '{action.Name}' action tool");
            sb.AppendLine($"    def {handlerName}(params):");
            AppendJsonComment(sb, action.ParametersSampleObject, "        ");
            sb.AppendLine($"        # TODO: handle '{action.Name}' action");
            sb.AppendLine($"        return 'done'");
            sb.AppendLine();
            sb.AppendLine($"    chat.handle('{action.Name}', {handlerName})");
            sb.AppendLine();
        }

        sb.AppendLine("    # Set user prompt and run");
        sb.AppendLine("    chat.set_user_prompt('Your question here')");
        sb.AppendLine("    result = chat.run()");
        sb.AppendLine("    answer = result.answer");

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
        var indented = string.Join(Environment.NewLine,
            pretty.Split(Environment.NewLine)
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
        foreach (var line in TryPrettyPrintJson(json).Split(Environment.NewLine))
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
