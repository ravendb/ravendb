using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.AI;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_26357 : RavenTestBase
{
    public RavenDB_26357(ITestOutputHelper output) : base(output)
    {
    }

    // For manual testing
    [RavenFact(RavenTestCategory.Ai, Skip = "Manual")]
    public async Task PrintOutputTest()
    {
        using var store = GetDocumentStore();

        var agentId = await CreateAgent(store, "OpenAi_ConnectionString");

        var resultCSharp = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        var resultPython = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "Python"));
        var resultJavascript = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "Javascript"));

        Console.WriteLine("C#");
        Console.WriteLine(resultCSharp.GeneratedCode);
        Console.WriteLine();
        Console.WriteLine("Python");
        Console.WriteLine(resultPython.GeneratedCode);
        Console.WriteLine();
        Console.WriteLine("JavaScript");
        Console.WriteLine(resultJavascript.GeneratedCode);
        Console.WriteLine();
    }


    [RavenTheory(RavenTestCategory.Ai)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CSharp_GeneratedCodeCompiles(bool useSchema)
    {
        using var store = GetDocumentStore();

        var agentId = await CreateAgent(store, "OpenAi_ConnectionString", useSchema);

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
    public async Task GenerateCode_CSharp()
    {
        const string expected = """"
                                using System;
                                using System.Collections.Generic;
                                using System.Threading.Tasks;
                                using Raven.Client.Documents;
                                using Raven.Client.Documents.AI;
                                using Raven.Client.Documents.Operations.AI.Agents;
                                
                                var agent = new AiAgentConfiguration
                                {
                                    Identifier = "user-info-agent-1",
                                    Name = "user-info-agent-1",
                                    ConnectionStringName = "OpenAi_ConnectionString",
                                    SystemPrompt = "Your role responsibility is to provide the user's name when requested.",
                                    SampleObject = """
                                    {
                                      "Answer": "Answer to the user question",
                                      "MoviesIds": [
                                        "The movies ids relevant to the query or response"
                                      ],
                                      "MoviesNames": [
                                        "The movies names relevant to the query or response"
                                      ]
                                    }
                                    """,
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
                                        new AiAgentToolAction
                                        {
                                            Name = "ChangeUserName",
                                            Description = "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.",
                                            ParametersSampleObject = """
                                            {
                                              "UserId": "Users/123456789",
                                              "NewUserName": "Jame's Smith",
                                              "OldUserName": "Jame's Parker"
                                            }
                                            """
                                        },
                                        new AiAgentToolAction
                                        {
                                            Name = "ChangeUserName2",
                                            Description = "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.",
                                            ParametersSampleObject = """
                                            {
                                              "UserId": "Users/123456789",
                                              "NewUserName": "Jame's Smith",
                                              "OldUserName": "Jame's Parker"
                                            }
                                            """
                                        }
                                    },
                                    SubAgents = new List<AiAgentToolSubAgent>
                                    {
                                        new AiAgentToolSubAgent
                                        {
                                            Identifier = "userAgent2Id",
                                            Description = "Use to ask about user name."
                                        }
                                    },
                                    Parameters = new List<AiAgentParameter>
                                    {
                                        new AiAgentParameter
                                        {
                                            Name = "currentUserId",
                                            Description = "the id of the current user that you talk with"
                                        },
                                        new AiAgentParameter
                                        {
                                            Name = "userId",
                                            Description = "the id of the user that you talk with"
                                        }
                                    },
                                    ChatTrimming = new AiAgentChatTrimmingConfiguration
                                    {
                                        History = new AiAgentHistoryConfiguration
                                        {
                                            HistoryExpirationInSec = 86400
                                        }
                                    }
                                };
                                
                                var documentStore = new DocumentStore
                                {
                                    Urls = new[] { "http://localhost:8080" },
                                    Database = "TestDB"
                                }.Initialize();
                                
                                // Create/deploy the agent
                                await documentStore.AI.CreateAgentAsync(agent);
                                
                                // Create a conversation/chat with the agent
                                var conversation = documentStore.AI.Conversation(
                                    agentId: "user-info-agent-1",
                                    conversationId: "Conversations/",
                                    new AiConversationCreationOptions()
                                        .AddParameter("currentUserId", "your-currentUserId-here")  // the id of the current user that you talk with
                                        .AddParameter("userId", "your-userId-here")  // the id of the user that you talk with
                                
                                );
                                
                                class ActionToolResult
                                {
                                    public bool IsSuccessful { get; set; }
                                    public string Answer { get; set; }
                                }
                                class ChangeUserNameArgs
                                {
                                    public string UserId { get; set; }
                                    public string NewUserName { get; set; }
                                    public string OldUserName { get; set; }
                                }
                                
                                // Define a handler for the "ChangeUserName" action tool
                                conversation.Handle<ChangeUserNameArgs, ActionToolResult>("ChangeUserName", async (args) =>
                                {
                                    // TODO: handle "ChangeUserName" action
                                    return new ActionToolResult { IsSuccessful = true };
                                });
                                
                                class ChangeUserName2Args
                                {
                                    public string UserId { get; set; }
                                    public string NewUserName { get; set; }
                                    public string OldUserName { get; set; }
                                }
                                
                                // Define a handler for the "ChangeUserName2" action tool
                                conversation.Handle<ChangeUserName2Args, ActionToolResult>("ChangeUserName2", async (args) =>
                                {
                                    // TODO: handle "ChangeUserName2" action
                                    return new ActionToolResult { IsSuccessful = true };
                                });
                                
                                
                                
                                // Set user prompt and run
                                conversation.SetUserPrompt("Your question here");
                                
                                class AgentResponse
                                {
                                    public string Answer { get; set; }
                                    public List<string> MoviesIds { get; set; }
                                    public List<string> MoviesNames { get; set; }
                                }
                                var result = await conversation.RunAsync<AgentResponse>();
                                var answer = result.Answer;
                                """"; 


        using var store = GetDocumentStore();
        var agentId = await CreateAgent(store, "OpenAi_ConnectionString");

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        Assert.Equal(expected.ReplaceLineEndings("\n"), result.GeneratedCode.ReplaceLineEndings("\n"));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_Python()
    {
        const string expected = """"
                                from ravendb import *
                                
                                document_store = DocumentStore(
                                    urls=["http://127.0.0.1:8080"],
                                    database="YourDatabaseName"
                                )
                                document_store.initialize()
                                
                                agent = AiAgentConfiguration(
                                    identifier='user-info-agent-1',
                                    name='user-info-agent-1',
                                    connection_string_name='OpenAi_ConnectionString',
                                    system_prompt="""Your role responsibility is to provide the user's name when requested.""",
                                    sample_object="""
                                    {
                                      "Answer": "Answer to the user question",
                                      "MoviesIds": [
                                        "The movies ids relevant to the query or response"
                                      ],
                                      "MoviesNames": [
                                        "The movies names relevant to the query or response"
                                      ]
                                    }
                                    """,
                                    queries=[
                                        AiAgentToolQuery(
                                            name='GetUserName',
                                            description='Get the user name',
                                            query='from Users where id() = $userId select Name',
                                            parameters_sample_object='{}'
                                        )
                                    ],
                                    actions=[
                                        AiAgentToolAction(
                                            name='ChangeUserName',
                                            description='Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.',
                                            parameters_sample_object="""
                                            {
                                              "UserId": "Users/123456789",
                                              "NewUserName": "Jame's Smith",
                                              "OldUserName": "Jame's Parker"
                                            }
                                            """
                                        ),
                                        AiAgentToolAction(
                                            name='ChangeUserName2',
                                            description='Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.',
                                            parameters_sample_object="""
                                            {
                                              "UserId": "Users/123456789",
                                              "NewUserName": "Jame's Smith",
                                              "OldUserName": "Jame's Parker"
                                            }
                                            """
                                        )
                                    ],
                                    sub_agents=[
                                        AiAgentToolSubAgent(
                                            identifier='userAgent2Id',
                                            description='Use to ask about user name.'
                                        )
                                    ],
                                    parameters=[
                                        AiAgentParameter(
                                            name='currentUserId',
                                            description='the id of the current user that you talk with'
                                        ),
                                        AiAgentParameter(
                                            name='userId',
                                            description='the id of the user that you talk with'
                                        )
                                    ],
                                    chat_trimming=AiAgentChatTrimmingConfiguration(
                                        history=AiAgentHistoryConfiguration(
                                            history_expiration_in_sec=86400
                                        )
                                    )
                                )
                                
                                # Create/deploy the agent
                                agent_id = document_store.ai.add_or_update_agent(agent).identifier
                                
                                # Create a conversation/chat with the agent
                                with document_store.ai.conversation(
                                    agent_id,
                                    conversation_id='Conversations/',
                                    parameters={
                                        'currentUserId': 'your-currentUserId-here',  # the id of the current user that you talk with
                                        'userId': 'your-userId-here',  # the id of the user that you talk with
                                    }
                                ) as chat:
                                
                                    # Define a handler for the 'ChangeUserName' action tool
                                    def handle_change_user_name(params):
                                        # {
                                        #   "UserId": "Users/123456789",
                                        #   "NewUserName": "Jame's Smith",
                                        #   "OldUserName": "Jame's Parker"
                                        # }
                                
                                        # TODO: handle 'ChangeUserName' action
                                        return 'done'
                                
                                    chat.handle('ChangeUserName', handle_change_user_name)
                                
                                    # Define a handler for the 'ChangeUserName2' action tool
                                    def handle_change_user_name2(params):
                                        # {
                                        #   "UserId": "Users/123456789",
                                        #   "NewUserName": "Jame's Smith",
                                        #   "OldUserName": "Jame's Parker"
                                        # }
                                
                                        # TODO: handle 'ChangeUserName2' action
                                        return 'done'
                                
                                    chat.handle('ChangeUserName2', handle_change_user_name2)
                                
                                
                                    # Set user prompt and run
                                    chat.set_user_prompt('Your question here')
                                    result = chat.run()
                                    answer = result.answer
                                """";

        using var store = GetDocumentStore();
        var agentId = await CreateAgent(store, "OpenAi_ConnectionString");

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "python"));
        Assert.Equal(expected.ReplaceLineEndings("\n"), result.GeneratedCode.ReplaceLineEndings("\n"));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_NodeJs()
    {
        const string expected = """"
                                const { DocumentStore } = require('ravendb');
                                
                                async function runConversation() {
                                    const agent =
                                    {
                                        identifier: 'user-info-agent-1',
                                        name: 'user-info-agent-1',
                                        connectionStringName: 'OpenAi_ConnectionString',
                                        systemPrompt: 'Your role responsibility is to provide the user\'s name when requested.',
                                        sampleObject: `
                                        {
                                          "Answer": "Answer to the user question",
                                          "MoviesIds": [
                                            "The movies ids relevant to the query or response"
                                          ],
                                          "MoviesNames": [
                                            "The movies names relevant to the query or response"
                                          ]
                                        }
                                        `,
                                        queries: [
                                            {
                                                name: 'GetUserName',
                                                description: 'Get the user name',
                                                query: 'from Users where id() = $userId select Name',
                                                parametersSampleObject: '{}'
                                            }
                                        ],
                                        actions: [
                                            {
                                                name: 'ChangeUserName',
                                                description: 'Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.',
                                                parametersSampleObject: `
                                                {
                                                  "UserId": "Users/123456789",
                                                  "NewUserName": "Jame's Smith",
                                                  "OldUserName": "Jame's Parker"
                                                }
                                                `
                                            },
                                            {
                                                name: 'ChangeUserName2',
                                                description: 'Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.',
                                                parametersSampleObject: `
                                                {
                                                  "UserId": "Users/123456789",
                                                  "NewUserName": "Jame's Smith",
                                                  "OldUserName": "Jame's Parker"
                                                }
                                                `
                                            }
                                        ],
                                        subAgents: [
                                            {
                                                identifier: 'userAgent2Id',
                                                description: 'Use to ask about user name.'
                                            }
                                        ],
                                        parameters: [
                                            {
                                                name: 'currentUserId',
                                                description: 'the id of the current user that you talk with'
                                            },
                                            {
                                                name: 'userId',
                                                description: 'the id of the user that you talk with'
                                            }
                                        ],
                                        chatTrimming: {
                                            history: {
                                                historyExpirationInSec: 86400
                                            }
                                        }
                                    };
                                
                                    const documentStore = new DocumentStore('http://localhost:8080', 'YourDatabase');
                                    documentStore.initialize();
                                
                                    // Create/deploy the agent
                                    const createdAgentResult = await documentStore.ai.createAgent(agent);
                                
                                    // Create a conversation/chat with the agent
                                    const chat = documentStore.ai.conversation(
                                        createdAgentResult.identifier,  // The agent ID
                                        'Conversations/',                // The conversation document prefix
                                        {
                                            parameters: {
                                                currentUserId: 'your-currentUserId-here',  // the id of the current user that you talk with
                                                userId: 'your-userId-here'  // the id of the user that you talk with
                                            }
                                        }
                                
                                    );
                                
                                    // Define a handler for the 'ChangeUserName' action tool
                                    chat.handle('ChangeUserName', async (params) => {
                                        // {
                                        //   "UserId": "Users/123456789",
                                        //   "NewUserName": "Jame's Smith",
                                        //   "OldUserName": "Jame's Parker"
                                        // }
                                
                                        // TODO: handle 'ChangeUserName' action
                                        return 'done';
                                    });
                                
                                    // Define a handler for the 'ChangeUserName2' action tool
                                    chat.handle('ChangeUserName2', async (params) => {
                                        // {
                                        //   "UserId": "Users/123456789",
                                        //   "NewUserName": "Jame's Smith",
                                        //   "OldUserName": "Jame's Parker"
                                        // }
                                
                                        // TODO: handle 'ChangeUserName2' action
                                        return 'done';
                                    });
                                
                                
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
                                """";

        using var store = GetDocumentStore();
        var agentId = await CreateAgent(store, "OpenAi_ConnectionString");

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "javascript"));
        Assert.Equal(expected.ReplaceLineEndings("\n"), result.GeneratedCode.ReplaceLineEndings("\n"));
    }


    [RavenFact(RavenTestCategory.Ai)]
    public async Task GenerateCode_UnsupportedLanguage_Throws()
    {
        using var store = GetDocumentStore();
        var agentId = await CreateAgent(store, "OpenAi_ConnectionString");

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

    // ---- Triple-quote fence tests ----

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CSharp_TripleQuoteInSystemPrompt_ProducesLongerFenceAndCompiles()
    {
        // A system prompt that literally contains """ is a correctness edge-case: the generated
        // C# raw-string literal must use a fence longer than the longest run of " in the content.
        const string agentId = "triple-quote-csharp-agent";
        var agent = new AiAgentConfiguration(agentId, "SomeConn",
            "You must always wrap your answer in \"\"\"triple\"\"\" quotes.")
        {
            // SampleObject is required so the generator emits the AgentResponse class
            // (without it RunAsync<AgentResponse> references an undefined type).
            SampleObject = """{"answer": "text"}"""
        };

        using var store = GetDocumentStore();
        await store.AI.CreateAgentAsync(agent);

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        var code = result.GeneratedCode;

        // The system prompt contains """ (3 quotes), so the fence must be """" (4 quotes).
        Assert.Contains("\"\"\"\"", code);

        // The raw triple-quotes from the prompt must appear unescaped inside the longer fence.
        Assert.Contains("\"\"\"triple\"\"\"", code);

        // The generated code must still compile.
        var wrappedCode = WrapInMainMethod(code);
        AssertCompilesWithRoslyn(wrappedCode);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Python_TripleQuoteInSystemPrompt_IsEscaped()
    {
        // A system prompt containing """ must have the triple-quotes escaped as \"\"\" so the
        // triple-double-quote fence is never accidentally closed.
        const string agentId = "triple-quote-python-agent";
        var agent = new AiAgentConfiguration(agentId, "SomeConn",
            "You must always wrap your answer in \"\"\"triple\"\"\" quotes.")
        {
            SampleObject = """{"answer": "text"}"""
        };

        using var store = GetDocumentStore();
        await store.AI.CreateAgentAsync(agent);

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "python"));
        var code = result.GeneratedCode;

        // Each """ from the prompt should be escaped to \"\"\" in the Python source.
        Assert.Contains("\\\"\\\"\\\"triple\\\"\\\"\\\"", code);

        // No raw, unescaped """ should appear *inside* the system_prompt value
        // (the fence markers themselves are fine; they don't contain the word "triple").
        Assert.DoesNotContain("\"\"\"triple", code);
    }

    // ---- Identifier sanitization tests ----

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CSharp_ActionNameWithSpecialChars_SanitizesToValidIdentifierAndCompiles()
    {
        // An action name such as "do-the-thing" contains a hyphen, which is not a valid C# identifier
        // character. The generator must replace it with '_' for the generated class name, while
        // keeping the original name in the Handle(...) string literal.
        const string agentId = "special-chars-csharp-agent";
        var agent = new AiAgentConfiguration(agentId, "SomeConn", "Some prompt.")
        {
            // SampleObject is required so the generator emits the AgentResponse class.
            SampleObject = """{"answer": "text"}""",
            Actions =
            [
                new AiAgentToolAction("do-the-thing", "Does the thing.")
                {
                    ParametersSampleObject = """{"input": "value"}"""
                }
            ]
        };

        using var store = GetDocumentStore();
        await store.AI.CreateAgentAsync(agent);

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        var code = result.GeneratedCode;

        // Class name must be a valid identifier — hyphen replaced with underscore.
        Assert.Contains("class do_the_thingArgs", code);

        // The Handle call must still reference the original action name as a string literal.
        Assert.Contains("\"do-the-thing\"", code);

        // The whole file must compile.
        AssertCompilesWithRoslyn(WrapInMainMethod(code));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Python_ActionNameWithSpecialChars_SanitizesToValidIdentifier()
    {
        // An action name such as "do-the-thing" must produce a valid Python function name.
        const string agentId = "special-chars-python-agent";
        var agent = new AiAgentConfiguration(agentId, "SomeConn", "Some prompt.")
        {
            SampleObject = """{"answer": "text"}""",
            Actions =
            [
                new AiAgentToolAction("do-the-thing", "Does the thing.")
            ]
        };

        using var store = GetDocumentStore();
        await store.AI.CreateAgentAsync(agent);

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "python"));
        var code = result.GeneratedCode;

        // Handler function must be a valid Python identifier — hyphen replaced with underscore.
        Assert.Contains("def handle_do_the_thing(params)", code);

        // The chat.handle call must still use the original action name.
        Assert.Contains("chat.handle('do-the-thing',", code);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CSharp_JsonPropertyWithInvalidName_EmitsJsonPropertyNameAttributeAndCompiles()
    {
        // JSON property names such as "my-key" (hyphen) or "class" (C# keyword) are not valid
        // C# identifiers. The generator must emit a [JsonPropertyName] attribute and sanitize
        // the property name so the generated class compiles cleanly.
        const string agentId = "invalid-json-prop-csharp-agent";
        var agent = new AiAgentConfiguration(agentId, "SomeConn", "Some prompt.")
        {
            SampleObject = """{"my-key": "hello", "class": "keyword-clash"}"""
        };

        using var store = GetDocumentStore();
        await store.AI.CreateAgentAsync(agent);

        var result = await store.Maintenance.SendAsync(new GenerateCodeAiAgentsOperation(agentId, "c#"));
        var code = result.GeneratedCode;

        // Hyphenated key: attribute preserves original name, property uses sanitized identifier.
        Assert.Contains("[System.Text.Json.Serialization.JsonPropertyName(\"my-key\")]", code);
        Assert.Contains("public string my_key { get; set; }", code);

        // Keyword clash: attribute preserves "class", property uses @class.
        Assert.Contains("[System.Text.Json.Serialization.JsonPropertyName(\"class\")]", code);
        Assert.Contains("public string @class { get; set; }", code);

        // The whole file must compile.
        AssertCompilesWithRoslyn(WrapInMainMethod(code));
    }

    // ---- Shared compilation helpers ----

    private static string WrapInMainMethod(string generatedCode)
    {
        var wrapped = generatedCode.Replace(
            "using Raven.Client.Documents.Operations.AI.Agents;",
            """
            using Raven.Client.Documents.Operations.AI.Agents;
            public class Program
            {
                public static async Task Main()
                {
            """);
        wrapped += """

            }
        }
        """;
        return ExtractClassesFromMain(wrapped);
    }

    private static void AssertCompilesWithRoslyn(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

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
            Assert.Fail($"Generated code failed to compile:\n{string.Join("\n", errors)}\n\nGenerated source:\n{code}");
        }
    }

    private static async Task<string> CreateAgent(IDocumentStore store, string connectionStringName, bool useSchema = false)
    {
        string changeUserNameSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance);
        string moviesOutputSampleObject = JsonConvert.SerializeObject(MoviesSampleObject.Instance);

        string changeUserNameParametersSchema = ChatCompletionClient.GetSchemaForTool(null, changeUserNameSampleObject);
        string moviesOutputSchema = ChatCompletionClient.GetSchemaFromSampleObject(moviesOutputSampleObject);

        var agent = new AiAgentConfiguration("user-info-agent-1",
            connectionStringName,
            "Your role responsibility is to provide the user's name when requested.")
        {
            OutputSchema = useSchema ? moviesOutputSchema : null,
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
                    ParametersSampleObject = useSchema ? null : changeUserNameSampleObject,
                    ParametersSchema = useSchema ? changeUserNameParametersSchema : null
                },
                new AiAgentToolAction("ChangeUserName2",
                    "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                {
                    ParametersSampleObject = useSchema ? null : changeUserNameSampleObject,
                    ParametersSchema = useSchema ? changeUserNameParametersSchema : null
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

        if (useSchema)
            return (await store.AI.CreateAgentAsync(agent)).Identifier;

        return (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;
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

