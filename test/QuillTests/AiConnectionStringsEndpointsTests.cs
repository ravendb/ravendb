using System.Net;
using System.Text;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillAiConnectionStringsCollection.Name)]
public class AiConnectionStringsEndpointsTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_creates_openai_connection_string()
    {
        var created = await Host.PostConnectionStringAsync(OpenAiCs("demo-llm"));
        Assert.Equal("demo-llm", created.Name);

        var aiCs = await Host.GetConnectionStringAsync("demo-llm");
        Assert.Equal(AiModelType.Chat, aiCs.ModelType);
        Assert.NotNull(aiCs.OpenAiSettings);
        Assert.Equal("gpt-4o-mini", aiCs.OpenAiSettings.Model);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_creates_azure_openai_connection_string()
    {
        var created = await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "azure-llm",
            ModelType = AiModelType.Chat,
            AzureOpenAiSettings = new AzureOpenAiSettings
            {
                ApiKey = "azure-key",
                Endpoint = "https://contoso.openai.azure.com/",
                Model = "gpt-4o-mini",
                DeploymentName = "gpt-4o-mini",
            },
        });
        Assert.Equal("azure-llm", created.Name);

        var aiCs = await Host.GetConnectionStringAsync("azure-llm");
        Assert.Equal(AiModelType.Chat, aiCs.ModelType);
        Assert.NotNull(aiCs.AzureOpenAiSettings);
        Assert.Equal("gpt-4o-mini", aiCs.AzureOpenAiSettings.DeploymentName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_creates_ollama_connection_string()
    {
        var created = await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "ollama-cs",
            ModelType = AiModelType.Chat,
            OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/", Model = "llama3.1" },
        });
        Assert.Equal("ollama-cs", created.Name);

        var aiCs = await Host.GetConnectionStringAsync("ollama-cs");
        Assert.Equal(AiModelType.Chat, aiCs.ModelType);
        Assert.NotNull(aiCs.OllamaSettings);
        Assert.Equal("llama3.1", aiCs.OllamaSettings.Model);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_returns_400_for_empty_body()
    {
        // raw: exercises minimal-API binding of an empty/absent body
        var emptyResp = await Host.Client.PostAsync(
            QuillRoutes.ConnectionStrings, new StringContent("", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, emptyResp.StatusCode);

        var nullResp = await Host.Client.PostAsync(
            QuillRoutes.ConnectionStrings, new StringContent("null", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, nullResp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_rejects_empty_name()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "",
            ModelType = AiModelType.Chat,
            OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/", Model = "llama3.1" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_rejects_non_chat_model_type()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "embed-llm",
            ModelType = AiModelType.TextEmbeddings,
            OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/", Model = "text-embedding-3-small" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("mistral")]
    [InlineData("google")]
    public async Task Post_rejects_provider_outside_the_supported_set(string provider)
    {
        // body substring pins the allow-list gate, not an earlier 400
        var cs = new AiConnectionString { Name = $"{provider}-cs", ModelType = AiModelType.Chat };
        switch (provider)
        {
            case "mistral":
                cs.MistralAiSettings = new MistralAiSettings { ApiKey = "mistral-key", Endpoint = "https://api.mistral.ai/v1/", Model = "mistral-tiny" };
                break;
            case "google":
                cs.GoogleSettings = new GoogleSettings { ApiKey = "g-key", Endpoint = "https://generativelanguage.googleapis.com/v1/", Model = "gemini-1.5-flash" };
                break;
        }

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostConnectionStringAsync(cs));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("unsupported provider", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_rejects_multiple_providers()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "mixed-llm",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/", Model = "gpt-4o-mini" },
            OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/", Model = "llama3.1" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_rejects_empty_openai_api_key()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "openai-llm",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings { ApiKey = "", Endpoint = "https://api.openai.com/v1/", Model = "gpt-4o-mini" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Test_reports_failure_when_the_provider_is_unreachable()
    {
        var result = await Host.TestConnectionStringAsync(new AiConnectionString
        {
            Name = "probe-ollama-cs",
            ModelType = AiModelType.Chat,
            OllamaSettings = new OllamaSettings { Uri = "http://localhost:11434/", Model = "llama3.1" },
        });

        Assert.False(result.Success);
        Assert.False(string.IsNullOrWhiteSpace(result.Error));

        Assert.DoesNotContain("Could not reach the provider", result.Error);
        Assert.DoesNotContain("Could not read the model test result", result.Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Test_rejects_provider_outside_the_supported_set()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.TestConnectionStringAsync(new AiConnectionString
        {
            Name = "probe-google-cs",
            ModelType = AiModelType.Chat,
            GoogleSettings = new GoogleSettings { ApiKey = "g-key", Endpoint = "https://generativelanguage.googleapis.com/v1/", Model = "gemini-1.5-flash" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("unsupported provider", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Test_rejects_non_chat_model_type()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.TestConnectionStringAsync(new AiConnectionString
        {
            Name = "probe-embed-cs",
            ModelType = AiModelType.TextEmbeddings,
            OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/", Model = "text-embedding-3-small" },
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Test_returns_400_for_null_body()
    {
        var resp = await Host.Client.PostAsync(
            QuillRoutes.ConnectionStringsTest, new StringContent("null", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task List_returns_created_connection_strings()
    {
        await Host.PostConnectionStringAsync(OpenAiCs("list-demo-llm"));
        await Host.PostConnectionStringAsync(OpenAiCs("list-ops-llm"));

        var list = await Host.GetConnectionStringsAsync();
        var names = list.Select(c => c.Name).ToArray();

        // this class shares one server, so sibling tests' strings may be present too
        Assert.Contains("list-demo-llm", names);
        Assert.Contains("list-ops-llm", names);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetByName_returns_openai_connection_string()
    {
        await Host.PostConnectionStringAsync(OpenAiCs("get-demo-llm"));

        var cs = await Host.GetConnectionStringAsync("get-demo-llm");
        Assert.Equal("get-demo-llm", cs.Name);
        Assert.Equal("gpt-4o-mini", cs.OpenAiSettings!.Model);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetByName_returns_provider_api_key_to_authenticated_admin()
    {
        await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "key-openai-llm",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings { ApiKey = "sk-real-test-key", Endpoint = "https://api.openai.com/v1/", Model = "gpt-4o-mini" },
        });

        // returns the full provider key by design — edit form pre-fills from it
        var cs = await Host.GetConnectionStringAsync("key-openai-llm");
        Assert.Equal("sk-real-test-key", cs.OpenAiSettings!.ApiKey);
        Assert.Equal("gpt-4o-mini", cs.OpenAiSettings.Model);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetByName_returns_404_for_unknown_name()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetConnectionStringAsync("ghost-llm"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_removes_connection_string()
    {
        // own name: the class shares a server, so deleting a name a sibling test created would be order-dependent
        await Host.PostConnectionStringAsync(OpenAiCs("delete-me-llm"));

        await Host.DeleteConnectionStringAsync("delete-me-llm");

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetConnectionStringAsync("delete-me-llm"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_404_for_unknown_name()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.DeleteConnectionStringAsync("ghost-llm"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_409_when_referenced_by_agent()
    {
        await using var app = await NewAppAsync(Host);

        await Host.PostConnectionStringAsync(OpenAiCs("referenced-llm"));

        // a server-wide CS lands in an app DB under the product's prefix, not the bare name
        var prefixed = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("referenced-llm");
        var csName = (await app.GetConnectionStringsAsync())
            .Single(c => c.Name == "referenced-llm" || c.Name == prefixed).Name;
        var provisioned = await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Name = "Support Bot",
            SystemPrompt = "You help.",
            ConnectionStringName = csName,
        });

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.DeleteConnectionStringAsync("referenced-llm"));
        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);

        Assert.Contains(provisioned.AgentId, ex.Body);
    }

    private static AiConnectionString OpenAiCs(string name) => new()
    {
        Name = name,
        ModelType = AiModelType.Chat,
        OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Endpoint = "https://api.openai.com/v1/", Model = "gpt-4o-mini" },
    };
}
