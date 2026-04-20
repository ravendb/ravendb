using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FastTests;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_26357 : RavenTestBase
{
    public RavenDB_26357(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CSharp_GeneratedCodeCompiles()
    {
        using var store = GetDocumentStore();

        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));

        var generatedCode = result.GeneratedCode;
        generatedCode = generatedCode.Replace("using Raven.Client.Documents.Operations.AI.Agents;", """
using Raven.Client.Documents.Operations.AI.Agents; 
public class Program
{ 
    public static async Task Main()
    {
""");
        generatedCode += """

    }
}
""";
        generatedCode = ExtractClassesFromMain(generatedCode);

        var syntaxTree = CSharpSyntaxTree.ParseText(generatedCode);

        // Gather the references needed to compile the generated code
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => a.IsDynamic == false && string.IsNullOrEmpty(a.Location) == false)
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Cast<MetadataReference>()
            .ToList();

        var compilation = CSharpCompilation.Create(
            assemblyName: "GeneratedAgentTestAssembly",
            syntaxTrees: [syntaxTree],
            references: references,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        using var ms = new MemoryStream();
        var emitResult = compilation.Emit(ms);

        if (emitResult.Success == false)
        {
            var errors = emitResult.Diagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Select(d => d.ToString());

            Assert.Fail($"Generated code failed to compile:\n{string.Join("\n", errors)}\n\nGenerated source:\n{generatedCode}");
        }
    }

    public static string ExtractClassesFromMain(string code)
    {
        int mainStart = code.IndexOf("public static async Task Main()");
        if (mainStart == -1)
            return code;

        int bodyStart = code.IndexOf('{', mainStart);
        int bodyEnd = FindMatchingBrace(code, bodyStart);

        if (bodyStart == -1 || bodyEnd == -1)
            return code;

        var mainBody = code.Substring(bodyStart + 1, bodyEnd - bodyStart - 1);

        var extractedClasses = new StringBuilder();
        var cleanedMain = new StringBuilder();

        int i = 0;
        while (i < mainBody.Length)
        {
            if (IsClassAt(mainBody, i))
            {
                int classStart = i;
                int braceStart = mainBody.IndexOf('{', i);
                int classEnd = FindMatchingBrace(mainBody, braceStart);

                var classText = mainBody.Substring(classStart, classEnd - classStart + 1);
                extractedClasses.AppendLine(classText).AppendLine();

                i = classEnd + 1;
            }
            else
            {
                cleanedMain.Append(mainBody[i]);
                i++;
            }
        }

        // rebuild code
        var result = new StringBuilder();
        result.Append(code.Substring(0, bodyStart + 1));
        result.Append(cleanedMain);
        result.Append(code.Substring(bodyEnd));

        result.AppendLine("\n// Extracted classes");
        result.AppendLine(extractedClasses.ToString());

        return result.ToString();
    }

    private static bool IsClassAt(string text, int index)
    {
        return text.Substring(index).StartsWith("class ");
    }

    private static int FindMatchingBrace(string text, int start)
    {
        int depth = 0;

        for (int i = start; i < text.Length; i++)
        {
            if (text[i] == '{')
                depth++;
            else if (text[i] == '}')
                depth--;

            if (depth == 0)
                return i;
        }

        return -1;
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_CSharp_ContainsAgentProperties()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));

        Assert.Contains("user-info-agent-1", result.GeneratedCode);
        Assert.Contains("OpenAi_ConnectionString", result.GeneratedCode);
        Assert.Contains("GetUserName", result.GeneratedCode);
        Assert.Contains("ChangeUserName", result.GeneratedCode);
        Assert.Contains("currentUserId", result.GeneratedCode);
        Assert.Contains("userId", result.GeneratedCode);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_Python_ContainsAgentProperties()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "python"));

        Assert.Contains("user-info-agent-1", result.GeneratedCode);
        Assert.Contains("OpenAi_ConnectionString", result.GeneratedCode);
        Assert.Contains("GetUserName", result.GeneratedCode);
        Assert.Contains("ChangeUserName", result.GeneratedCode);
        Assert.Contains("currentUserId", result.GeneratedCode);
        Assert.Contains("userId", result.GeneratedCode);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_NodeJs_ContainsAgentProperties()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "javascript"));

        Assert.Contains("user-info-agent-1", result.GeneratedCode);
        Assert.Contains("OpenAi_ConnectionString", result.GeneratedCode);
        Assert.Contains("GetUserName", result.GeneratedCode);
        Assert.Contains("ChangeUserName", result.GeneratedCode);
        Assert.Contains("currentUserId", result.GeneratedCode);
        Assert.Contains("userId", result.GeneratedCode);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_CSharp_ContainsUsings()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));

        Assert.Contains("using System;", result.GeneratedCode);
        Assert.Contains("using System.Collections.Generic;", result.GeneratedCode);
        Assert.Contains("using Raven.Client.Documents.Operations.AI.Agents;", result.GeneratedCode);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_UnsupportedLanguage_Throws()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var ex = await Assert.ThrowsAsync<RavenException>(async () =>
            await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "ruby")));

        Assert.Contains("Unsupported language 'ruby'. Supported languages are: C#, JavaScript, Python.", ex.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_NonExistentAgent_Throws()
    {
        using var store = GetDocumentStore();

        var ex = await Assert.ThrowsAsync<RavenException>(async () =>
            await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation("non-existent-agent", "c#")));

        Assert.NotNull(ex);
        Assert.Contains("Agent 'non-existent-agent' doesn't exist", ex.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_LanguageIsCaseInsensitive()
    {
        using var store = GetDocumentStore();
        var agent = BuildAgent("OpenAi_ConnectionString");
        var agentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var lower = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        var upper = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "C#"));
        var mixed = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "Python"));

        Assert.Equal(lower.GeneratedCode, upper.GeneratedCode);
        Assert.NotNull(mixed.GeneratedCode);
    }

    private static AiAgentConfiguration BuildAgent(string connectionStringName)
    {
        var agent = new AiAgentConfiguration("user-info-agent-1",
            connectionStringName,
            "Your role responsibility is to provide the user's name when requested.")
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = "userAgent2Id",
                    Description = "Use to ask about user name."
                }
            ],
            Queries = new List<AiAgentToolQuery>
            {
                new AiAgentToolQuery
                {
                    Name = "GetUserName",
                    Description = "Get the user name",
                    Query = "from Users where id() = $userId select Name",
                    ParametersSampleObject = "{}"
                }
            },
            Actions = new List<AiAgentToolAction>
            {
                new AiAgentToolAction("ChangeUserName",
                    "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                },
                new AiAgentToolAction("ChangeUserName2",
                    "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                }
            },
            ChatTrimming = new AiAgentChatTrimmingConfiguration
            {
                Truncate = new AiAgentTruncateChat()
                {
                    MessagesLengthBeforeTruncate = 2,
                    MessagesLengthAfterTruncate = 2
                },
                History = new AiAgentHistoryConfiguration(TimeSpan.FromDays(1))
            }
        };
        agent.Parameters.Add(new AiAgentParameter("currentUserId", "the id of the current user that you talk with"));
        agent.Parameters.Add(new AiAgentParameter("userId", "the id of the user that you talk with"));
        return agent;
    }

    private class ChangeUserNameSampleRequest
    {
        public static readonly ChangeUserNameSampleRequest Instance = new()
        {
            UserId = "Users/123456789",
            OldUserName = "Jame's Parker",
            NewUserName = "Jame's Smith"
        };

        public string UserId { get; set; }
        public string NewUserName { get; set; }
        public string OldUserName { get; set; }
    }

    private class MoviesSampleObject
    {
        public static MoviesSampleObject Instance = new()
        {
            Answer = "Answer to the user question",
            MoviesIds = ["The movies ids relevant to the query or response"],
            MoviesNames = ["The movies names relevant to the query or response"]
        };

        public string Answer;

        public List<string> MoviesIds { get; set; }
        public List<string> MoviesNames { get; set; }

        public override string ToString()
        {
            return $"Answer: {Answer}, " +
                   $"MoviesIds: [{string.Join(", ", MoviesIds ?? new List<string>())}], " +
                   $"MoviesNames: [{string.Join(", ", MoviesNames ?? new List<string>())}]";
        }
    }

    private sealed class GenerateCodeAiAgentsOperation : IMaintenanceOperation<GenerateCodeAiAgentsResponse>
    {
        private readonly string _agentId;
        private readonly string _language;

        public GenerateCodeAiAgentsOperation()
        {
        }

        public GenerateCodeAiAgentsOperation(string agentId, string language)
        {
            ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
            ValidationMethods.AssertNotNullOrEmpty(language, nameof(language));
            _agentId = agentId;
            _language = language;
        }

        public RavenCommand<GenerateCodeAiAgentsResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GenerateCodeAiAgentOperationCommand(_agentId, _language);
        }

        private sealed class GenerateCodeAiAgentOperationCommand : RavenCommand<GenerateCodeAiAgentsResponse>
        {
            private readonly string _agentId;
            private readonly string _language;

            public GenerateCodeAiAgentOperationCommand(string agentId, string language)
            {
                _agentId = agentId;
                _language = language;
            }
            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/agent/generate-code?agentId={Uri.EscapeDataString(_agentId)}&language={Uri.EscapeDataString(_language)}";
                

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = GetGenerateCodeAiAgentsResponse(response);
            }

            private static readonly Func<BlittableJsonReaderObject, GenerateCodeAiAgentsResponse> GetGenerateCodeAiAgentsResponse = JsonDeserializationClient.GenerateJsonDeserializationRoutine<GenerateCodeAiAgentsResponse>();
        }
    }

    private class GenerateCodeAiAgentsResponse
    {
        public string GeneratedCode { get; set; }
    }

}

