using Xunit;

namespace QuillTests.E2E.Fixtures;

[CollectionDefinition(Name)]
public class QuillAiConnectionStringsCollection : ICollectionFixture<QuillCollectionHost>
{
    public const string Name = "quill-ai-connection-strings";
}

[CollectionDefinition(Name)]
public class QuillFanOutCollection : ICollectionFixture<QuillCollectionHost>
{
    public const string Name = "quill-fan-out";
}

[CollectionDefinition(Name)]
public class QuillWizardCollection : ICollectionFixture<QuillCollectionHost>
{
    public const string Name = "quill-wizard";
}

[CollectionDefinition(Name)]
public class QuillFeedbackCollection : ICollectionFixture<QuillFeedbackFixture>
{
    public const string Name = "quill-feedback";
}

[CollectionDefinition(Name)]
public class QuillAiModelsCollection : ICollectionFixture<QuillAiModelsFixture>
{
    public const string Name = "quill-ai-models";
}

[CollectionDefinition(Name)]
public class QuillSuggestAgentCollection : ICollectionFixture<QuillAiHelperFixture>
{
    public const string Name = "quill-suggest-agent";
}

[CollectionDefinition(Name)]
public class QuillSuggestCdcCollection : ICollectionFixture<QuillAiHelperFixture>
{
    public const string Name = "quill-suggest-cdc";
}

[CollectionDefinition(Name)]
public class QuillAssistantCollection : ICollectionFixture<QuillAiHelperFixture>
{
    public const string Name = "quill-assistant";
}

// its own host: these tests register server-wide AI connection strings pointing at per-test mock LLMs
[CollectionDefinition(Name)]
public class QuillAgentActionsCollection : ICollectionFixture<QuillCollectionHost>
{
    public const string Name = "quill-agent-actions";
}

[CollectionDefinition(Name)]
public class QuillTelegramCollection : ICollectionFixture<QuillTelegramFixture>
{
    public const string Name = "quill-telegram";
}

[CollectionDefinition(Name)]
public class QuillSlackCollection : ICollectionFixture<QuillSlackFixture>
{
    public const string Name = "quill-slack";
}

[CollectionDefinition(Name)]
public class QuillDiscordCollection : ICollectionFixture<QuillDiscordFixture>
{
    public const string Name = "quill-discord";
}
