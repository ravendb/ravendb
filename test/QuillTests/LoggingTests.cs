using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Auth;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;
// the tests speak RavenDB's level vocabulary, same as the product does now
using LogLevel = Sparrow.Logging.LogLevel;

namespace QuillTests;

/// <summary>
/// Asserts against the real log files rather than a test double, so the layout, the principal, the
/// redaction and the rotation settings are all covered. Every host gets its own temp directory and its
/// own NLog LogFactory, which is what keeps these parallel-safe.
/// </summary>
public class LoggingTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection), IClassFixture<QuillCollectionHost>
{
    [RavenFact(RavenTestCategory.Quill)]
    public void A_configuration_missing_a_rule_the_appliance_needs_falls_back_to_the_built_in_defaults()
    {
        using var logs = new TempLogDirectory();
        var path = logs.WriteConfig();

        File.WriteAllText(path, File.ReadAllText(path).Replace("Raven_Default_Audit", "Renamed_By_Operator"));

        var logging = QuillLogging.CreateOrFallback(new QuillLogOptions { ConfigPath = path });

        Assert.Equal(QuillLogSource.BuiltIn, logging.Source);
        Assert.Null(logging.LoadedFrom);
        Assert.Contains(logging.ConfigurationProblems, problem => problem.Contains("Raven_Default_Audit"));

        // the file said both sinks were on; the defaults it fell back to have them off
        Assert.False(logging.IsFileLogEnabled);
        Assert.False(logging.IsAuditEnabled);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_configuration_that_will_not_parse_falls_back_to_the_built_in_defaults()
    {
        using var logs = new TempLogDirectory();
        var path = logs.WriteConfig();

        File.WriteAllText(path, "<nlog><rules></nlog>");

        var logging = QuillLogging.CreateOrFallback(new QuillLogOptions { ConfigPath = path });

        Assert.Equal(QuillLogSource.BuiltIn, logging.Source);
        Assert.NotEmpty(logging.ConfigurationProblems);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_configuration_that_is_not_there_is_not_a_problem()
    {
        using var logs = new TempLogDirectory();

        var logging = QuillLogging.CreateOrFallback(new QuillLogOptions { ConfigPath = logs.ConfigPath });

        Assert.Equal(QuillLogSource.BuiltIn, logging.Source);
        Assert.True(logging.ConfigurationProblems.Count == 0,
            $"the built-in defaults reported: {string.Join("; ", logging.ConfigurationProblems)}");
        Assert.Equal(logs.ConfigPath, logging.ConfigPath);
    }

    /// A persisted change is written into a copy of this file, so it has to load and it has to carry the
    /// rules by the names the appliance looks up - otherwise the first persist is what discovers it.
    [RavenFact(RavenTestCategory.Quill)]
    public void The_shipped_template_loads_and_carries_the_rules_persist_needs()
    {
        var template = QuillLogging.Create(
            Path.Combine(AppContext.BaseDirectory, QuillLogging.TemplateFileName));

        try
        {
            Assert.True(template.ConfigurationProblems.Count == 0,
                $"the shipped template reported: {string.Join("; ", template.ConfigurationProblems)}");

            Assert.Equal(QuillLogSource.File, template.Source);
            Assert.Equal(LogLevel.Info, template.CurrentMinLevel);
            Assert.False(template.IsFileLogEnabled);
            Assert.False(template.IsAuditEnabled);
            Assert.False(template.MicrosoftEnabled);
        }
        finally
        {
            template.Factory.Shutdown();
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_setting_the_framework_cannot_apply_is_reported_not_swallowed()
    {
        using var logs = new TempLogDirectory();
        var path = logs.WriteConfig();

        File.WriteAllText(path, File.ReadAllText(path).Replace("archiveAboveSize=\"134217728\"",
            "archiveAboveSize=\"one hundred megabytes\""));   // throwConfigExceptions is false, so NLog degrades

        var logging = QuillLogging.Create(path);

        Assert.NotEmpty(logging.ConfigurationProblems);
        Assert.All(logging.ConfigurationProblems, problem => Assert.Contains("ArchiveAboveSize", problem));

        // NLog left -1 bytes, which divides down to 0 MB: broken is indistinguishable from unset
        Assert.Equal(0, logging.GetLogsConfiguration().ArchiveAboveSizeInMb);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_sink_wired_to_a_renamed_target_does_not_stop_the_appliance_starting()
    {
        using var logs = new TempLogDirectory();
        var path = logs.WriteConfig();

        // targets, unlike rules, are optional: a rule may point at a target of the operator's own, and the
        // sink then works while the appliance can no longer name its file
        File.WriteAllText(path, File.ReadAllText(path)
            .Replace("name=\"QuillLoggingAudit\"", "name=\"OperatorsOwnAudit\"")
            .Replace("writeTo=\"QuillLoggingAudit\"", "writeTo=\"OperatorsOwnAudit\"")
            .Replace("name=\"QuillLogging\"", "name=\"OperatorsOwnFile\""));

        var logging = QuillLogging.Create(path);

        Assert.True(logging.IsAuditEnabled);
        Assert.Null(logging.CurrentAuditFile);
        Assert.True(logging.IsFileLogEnabled);
        Assert.Null(logging.CurrentLogFile);

        await using var host = await NewLoggingHostAsync(logging);

        var normal = await logs.ReadNormalAsync(host, "there is no 'QuillLogging' target");
        Assert.Contains("cannot be reported or moved through the API", normal);
        Assert.Contains("AUDIT log started", logs.ReadAudit());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Audit_disabled_is_a_no_op_not_a_failure()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewHostAsync(configureServices: services =>
        {
            services.RemoveAll<QuillLogging>();
            services.AddSingleton(QuillLogging.Create(logs.WriteConfig(audit: false)));
        });

        var logging = host.Services.GetRequiredService<QuillLogging>();

        Assert.False(logging.IsAuditEnabled);
        Assert.Null(logging.CurrentAuditDirectory);

        logging.Audit("POST", "App 'nothing'", context: null);

        var app = await NewAppAsync(host);
        var agent = await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "no-audit-agent",
            Name = "No Audit Agent",
            SystemPrompt = "You are not audited.",
            ConnectionStringName = host.ConnectionStringName,
        });
        await app.DeleteAgentAsync(agent.AgentId);
        await host.DeleteAppAsync(app.Slug);

        Assert.False(logs.AuditFileExists());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Every_level_survives_a_set_then_read_round_trip()
    {
        using var logs = new TempLogDirectory();
        var logging = QuillLogging.Create(logs.WriteConfig(minLevel: LogLevel.Off));

        Assert.Equal(LogLevel.Off, logging.CurrentMinLevel);

        foreach (var level in Enum.GetValues<LogLevel>())
        {
            logging.ConfigureLogging(new UpdateLogConfigurationRequest(
                new LogsUpdate(Path: logs.FolderPath, MinLevel: level)));
            Assert.Equal(level, logging.CurrentMinLevel);
        }

        logging.ConfigureLogging(new UpdateLogConfigurationRequest(
            new LogsUpdate(logs.FolderPath, LogLevel.Debug)));
        Assert.True(logging.Factory.GetLogger("probe").IsDebugEnabled);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Audit_lines_match_the_ravendb_layout_and_stay_out_of_the_normal_log()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs);

        Assert.True(host.Services.GetRequiredService<QuillLogging>().IsAuditEnabled);

        await host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "layout-probe",
            ModelType = AiModelType.Chat,
            OllamaSettings = new OllamaSettings { Uri = "http://127.0.0.1:1/", Model = "llama3.1" },
        });

        var audit = await logs.ReadAuditAsync(host, "AiConnectionString 'layout-probe'");

        Assert.Contains("Date|Level|ThreadID|Resource|Component|Logger|Message|Data", audit);
        var line = SingleLineContaining(audit, "AiConnectionString 'layout-probe'");
        Assert.Contains("|Quill||Audit|", line);
        Assert.Contains($"{ApiKeyAuthenticationHandler.SchemeName} [operator]", line);
        Assert.Contains("POST AiConnectionString 'layout-probe' provider=Ollama", line);

        // the audit rule is Final AND ahead of the catch-all "*" rule, or every line is duplicated here
        var normal = logs.ReadNormalNow();
        Assert.DoesNotContain("|Audit|", normal);
        Assert.DoesNotContain("AiConnectionString 'layout-probe'", normal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Authentication_events_are_audited()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs);

        const string wrongKey = "wrong-key-that-must-not-be-logged";
        using var anonymous = host.Factory.CreateClient();
        anonymous.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        await anonymous.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = wrongKey });
        await anonymous.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        await anonymous.PostAsync(QuillRoutes.AuthLogout, content: null);

        using var badKeyClient = host.Factory.CreateClient();
        badKeyClient.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);
        badKeyClient.DefaultRequestHeaders.Add(ApiKeyAuthenticationHandler.HeaderName, wrongKey);
        await badKeyClient.GetAsync(QuillRoutes.Apps);

        using var noCredentialClient = host.Factory.CreateClient();
        noCredentialClient.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);
        await noCredentialClient.GetAsync(QuillRoutes.Apps);

        // the rejected key is the last audited event, so waiting on it settles the three before it
        var audit = await logs.ReadAuditAsync(host, "AUTH rejected (invalid API key)");

        Assert.Contains($"{QuillAudit.NoPrincipal}, LOGIN failed", audit);
        Assert.Contains("Cookies [operator], LOGIN succeeded", audit);
        Assert.Contains("LOGOUT session ended", audit);

        // a request carrying no credential at all is deliberately NOT audited: it is every anonymous probe
        // on the internet. Presenting a wrong key still is.
        Assert.DoesNotContain("AUTH challenged", audit);

        Assert.DoesNotContain(wrongKey, audit);
        Assert.DoesNotContain(ApplianceWebApplicationFactory.TestApiKey, audit);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_credential_in_a_webhook_url_is_not_described_as_its_origin()
    {
        var described = AgentActionBindings.DescribeTargetsForAudit(new Dictionary<string, WebhookBinding>
        {
            ["basic"] = ActionFixtures.Webhook("https://svc:s3cret@hooks.example.test/quill"),
            ["port"] = ActionFixtures.Webhook("http://hooks.example.test:8443/quill"),
            ["hostless"] = ActionFixtures.Webhook("file:///C:/logs/x"),
            ["junk"] = ActionFixtures.Webhook("not a url at all"),
            ["empty"] = ActionFixtures.Webhook("   "),
        });

        Assert.DoesNotContain("s3cret", described);
        Assert.DoesNotContain("svc", described);
        Assert.DoesNotContain("/quill", described);

        Assert.Contains("basic->https://hooks.example.test", described);
        Assert.Contains("port->http://hooks.example.test:8443", described);
        Assert.Contains("hostless->file:(no host)", described);
        Assert.Contains("junk->(unparsable)", described);
        Assert.Contains("empty->none", described);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Configuration_operations_are_audited_without_their_secrets()
    {
        const string providerKey = "sk-provider-key-must-not-be-logged";
        const string webhookSecret = "webhook-secret-must-not-be-logged";
        const string sqlPassword = "sql-password-must-not-be-logged";
        const string webhookUrl = "https://hooks.example.test/quill";

        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs, ownServer: true);

        var app = await NewAppAsync(host);

        // no reachable SQL server here; the audit line is written for the attempt either way
        try
        {
            await host.SetupConnectAsync(new ConnectRequest(
                "Npgsql",
                $"Host=db.example.test;Database=northwind;Username=svc;Password={sqlPassword}"));
        }
        catch (QuillHttpException)
        {
        }

        var sample = AiHelperSamples.BuildCdcConfig();
        await host.SetupMapAsync(new MapRequest
        {
            Slug = app.Slug,
            Name = $"{app.Slug}-cdc",
            ConnectionStringName = sample.ConnectionStringName,
            Tables = sample.Tables,
            Postgres = sample.Postgres,
            Disabled = true,
            SkipInitialLoad = true,
        });

        var agent = await app.ProvisionAgentAsync(new EditAgentRequest(
            new AiAgentConfiguration
            {
                Identifier = "audited-agent",
                Name = "Audited Agent",
                SystemPrompt = "You are audited.",
                ConnectionStringName = host.ConnectionStringName,
                Actions =
                [
                    new AiAgentToolAction
                    {
                        Name = "notify",
                        Description = "Notifies an external system.",
                        ParametersSampleObject = "{}",
                    },
                ],
            },
            ActionBindings: new Dictionary<string, WebhookBinding>
            {
                ["notify"] = ActionFixtures.Webhook(webhookUrl, webhookSecret),
            }));

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agent.AgentId, [], "Audited Channel"));

        var minted = await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channel.ChannelId, new Dictionary<string, string>(), 3600, 5));
        await app.RevokeEmbedLinkAsync(minted.Token);

        // after minting: a disabled channel refuses to mint, which is the product behaving correctly
        await app.UpdateChannelAsync(channel.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        await host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = "audited-cs",
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings { ApiKey = providerKey, Model = "gpt-4o" },
        });
        await host.DeleteConnectionStringAsync("audited-cs");

        await app.DeleteChannelAsync(channel.ChannelId);
        await app.DeleteAgentAsync(agent.AgentId);
        await host.DeleteAppAsync(app.Slug);

        var audit = await logs.ReadAuditAsync(host, $"DELETE App '{app.Slug}'");

        Assert.Contains($"POST App '{app.Slug}' provisioned", audit);
        Assert.Contains("WizardSource", audit);
        Assert.Contains($"POST WizardMapping '{app.Slug}' stored", audit);
        Assert.Contains($"POST AiAgentConfiguration '{agent.AgentId}' in App '{app.Slug}'", audit);
        Assert.Contains($"POST Channel '{channel.ChannelId}' in App '{app.Slug}' type=IFrame", audit);
        Assert.Contains($"PUT Channel '{channel.ChannelId}' in App '{app.Slug}' enabled=False", audit);
        Assert.Contains("POST EmbedLink minted", audit);
        Assert.Contains("DELETE EmbedLink revoked", audit);
        Assert.Contains("POST AiConnectionString 'audited-cs' provider=OpenAi", audit);
        Assert.Contains("DELETE AiConnectionString 'audited-cs'", audit);
        Assert.Contains($"DELETE Channel '{channel.ChannelId}' in App '{app.Slug}'", audit);
        Assert.Contains($"DELETE AiAgentConfiguration '{agent.AgentId}' in App '{app.Slug}'", audit);
        Assert.Contains($"DELETE App '{app.Slug}'", audit);

        Assert.DoesNotContain(providerKey, audit);
        Assert.DoesNotContain(webhookSecret, audit);
        Assert.DoesNotContain(sqlPassword, audit);
        Assert.DoesNotContain(minted.Token, audit);
        Assert.Contains(EmbedLink.RedactToken(minted.Token), audit);

        // for Slack/Teams-style hooks the URL path IS the credential, so only the origin is audited
        Assert.DoesNotContain(webhookUrl, audit);
        Assert.DoesNotContain("/quill", audit);
        Assert.Contains("https://hooks.example.test", audit);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_level_and_the_log_path_can_be_changed_from_the_api()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs);

        var logging = host.Services.GetRequiredService<QuillLogging>();
        var probe = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("LoggingTests.Probe");

        Assert.Equal(LogLevel.Info, logging.CurrentMinLevel);
        probe.LogInformation("information-before-change");
        probe.LogDebug("debug-while-information");

        var before = await QuillHttp.GetAsync<LogConfigurationResponse>(
            host.Client, QuillRoutes.SettingsLogConfiguration);
        Assert.Equal(LogLevel.Info, before.Logs.MinLevel);
        Assert.Equal(LogLevel.Info, before.Logs.CurrentMinLevel);
        Assert.Equal(LogLevel.Info, before.AuditLogs.Level);

        // the whole state, the way the FE sends it back: keeping the path is what keeps the sink on
        var raised = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { logs = new { path = logs.FolderPath, minLevel = nameof(LogLevel.Debug) } });
        Assert.Equal(System.Net.HttpStatusCode.NoContent, raised.StatusCode);

        probe.LogDebug("debug-after-change");

        var normal = await logs.ReadNormalAsync(host, "debug-after-change");
        Assert.Contains("information-before-change", normal);
        Assert.DoesNotContain("debug-while-information", normal);

        var after = await QuillHttp.GetAsync<LogConfigurationResponse>(
            host.Client, QuillRoutes.SettingsLogConfiguration);
        // nothing was persisted, so a restart still comes up at the level the file names
        Assert.Equal(LogLevel.Info, after.Logs.MinLevel);
        Assert.Equal(LogLevel.Debug, after.Logs.CurrentMinLevel);

        Assert.Contains("POST LogConfiguration", await logs.ReadAuditAsync(host, "minLevel=Debug"));

        var badLevel = await host.Client.PostAsJsonAsync(
            QuillRoutes.SettingsLogConfiguration, new { logs = new { minLevel = "Chatty" } });
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, badLevel.StatusCode);

        var blocker = Path.Combine(Path.GetTempPath(), $"quill-block-{Guid.NewGuid():N}");
        File.WriteAllText(blocker, "not a directory");
        try
        {
            var badPath = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
                new { logs = new { path = blocker, minLevel = nameof(LogLevel.Debug) } });

            Assert.Equal(System.Net.HttpStatusCode.BadRequest, badPath.StatusCode);
            Assert.Equal(logs.FolderPath, logging.CurrentLogDirectory);
        }
        finally
        {
            File.Delete(blocker);
        }

        using var moved = new TempLogDirectory();

        var move = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { logs = new { path = moved.FolderPath, minLevel = nameof(LogLevel.Info) } });

        Assert.Equal(System.Net.HttpStatusCode.NoContent, move.StatusCode);
        Assert.Equal(moved.FolderPath, logging.CurrentLogDirectory);

        probe.LogInformation("after-the-move");
        Assert.Contains("after-the-move", await moved.ReadNormalAsync(host, "after-the-move"));

        // no path switches the file sink off, leaving stdout alone
        var off = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { logs = new { minLevel = nameof(LogLevel.Info) } });

        Assert.Equal(System.Net.HttpStatusCode.NoContent, off.StatusCode);
        Assert.False(logging.IsFileLogEnabled);
        Assert.Null(logging.CurrentLogDirectory);
        Assert.Equal(LogLevel.Info, logging.CurrentMinLevel);

        var reported = await QuillHttp.GetAsync<LogConfigurationResponse>(
            host.Client, QuillRoutes.SettingsLogConfiguration);
        Assert.Null(reported.Logs.Path);

        probe.LogInformation("after-switching-off");
        logging.Flush();
        Assert.DoesNotContain("after-switching-off", moved.ReadNormalNow());

        // the audit sink is the file's business, not the API's, and stayed where the file put it
        Assert.Equal(logs.FolderPath, logging.CurrentAuditDirectory);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_shipped_configuration_reports_its_defaults_and_its_disabled_sinks()
    {
        var configuration = await QuillHttp.GetAsync<LogConfigurationResponse>(
            Host.Client, QuillRoutes.SettingsLogConfiguration);

        // no file by default - stdout is the appliance's log, and s6 already persists that stream
        Assert.Null(configuration.Logs.Path);
        Assert.Equal(LogLevel.Info, configuration.Logs.MinLevel);
        Assert.Equal(128, configuration.Logs.ArchiveAboveSizeInMb);
        Assert.Equal(3, configuration.Logs.MaxArchiveDays);
        Assert.Null(configuration.Logs.MaxArchiveFiles);

        // audit off is Level=Off with no path, not a null block, and its rotation is its own
        Assert.Equal(LogLevel.Off, configuration.AuditLogs.Level);
        Assert.Null(configuration.AuditLogs.Path);
        Assert.Equal(128, configuration.AuditLogs.ArchiveAboveSizeInMb);
        Assert.Equal(3, configuration.AuditLogs.MaxArchiveDays);
        Assert.Null(configuration.AuditLogs.MaxArchiveFiles);

        // Microsoft and System are not captured at all, the way RavenDB ships Logs.Microsoft.Enabled false
        Assert.Equal(LogLevel.Off, configuration.MicrosoftLogs.MinLevel);
        Assert.Equal(LogLevel.Off, configuration.MicrosoftLogs.CurrentMinLevel);

        // nothing was copied onto a volume, yet a persisted change still has somewhere to go
        Assert.True(configuration.CanPersist);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_microsoft_level_change_is_refused_while_framework_logging_is_switched_off()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(
            QuillLogging.Create(logs.WriteConfig(microsoft: false)));

        var refused = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { microsoftLogs = new { minLevel = nameof(LogLevel.Debug) } });

        Assert.Equal(System.Net.HttpStatusCode.BadRequest, refused.StatusCode);

        var error = await refused.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("Raven_Microsoft", error!.Error);

        var logging = host.Services.GetRequiredService<QuillLogging>();
        Assert.False(logging.MicrosoftEnabled);
        Assert.Equal(LogLevel.Off, logging.CurrentMicrosoftMinLevel);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Framework_categories_are_not_captured_while_they_are_switched_off()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(
            QuillLogging.Create(logs.WriteConfig(microsoft: false)));

        var factory = host.Services.GetRequiredService<ILoggerFactory>();
        factory.CreateLogger("Microsoft.AspNetCore.Whatever").LogError("microsoft-must-not-appear");
        factory.CreateLogger("System.Net.Whatever").LogError("system-must-not-appear");
        factory.CreateLogger("Quill.Probe").LogInformation("quill-must-appear");

        // waiting on the unsuppressed line proves the suppressed ones had their chance to land
        var normal = await logs.ReadNormalAsync(host, "quill-must-appear");
        Assert.DoesNotContain("microsoft-must-not-appear", normal);
        Assert.DoesNotContain("system-must-not-appear", normal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Persisting_with_no_file_yet_writes_what_is_running()
    {
        using var logs = new TempLogDirectory();
        Directory.CreateDirectory(logs.FolderPath);

        var options = new QuillLogOptions
        {
            ConfigPath = logs.ConfigPath,
            Path = logs.FolderPath,
            AuditPath = logs.FolderPath,
        };

        await using var host = await NewLoggingHostAsync(QuillLogging.CreateBuiltIn(options));

        Assert.False(File.Exists(logs.ConfigPath));

        var persisted = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { logs = new { path = logs.FolderPath, minLevel = nameof(LogLevel.Debug) }, persist = true });

        Assert.Equal(System.Net.HttpStatusCode.NoContent, persisted.StatusCode);
        Assert.True(File.Exists(logs.ConfigPath));

        var restarted = QuillLogging.CreateOrFallback(options);

        try
        {
            Assert.Equal(QuillLogSource.File, restarted.Source);
            Assert.Empty(restarted.ConfigurationProblems);

            // what was running is what came back: the raised level, the sink switched on, and the
            // defaults for everything the request never mentioned
            Assert.Equal(LogLevel.Debug, restarted.CurrentMinLevel);
            Assert.Equal(LogLevel.Debug, restarted.MinLevel);
            Assert.True(restarted.IsFileLogEnabled);
            Assert.Equal(logs.FolderPath, restarted.CurrentLogDirectory);
            Assert.False(restarted.IsAuditEnabled);
            Assert.False(restarted.MicrosoftEnabled);
            Assert.Equal(128, restarted.GetLogsConfiguration().ArchiveAboveSizeInMb);
            Assert.Equal(3, restarted.GetLogsConfiguration().MaxArchiveDays);
        }
        finally
        {
            restarted.Factory.Shutdown();
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failure_that_is_audited_keeps_its_exception_out_of_the_audit_log()
    {
        using var logs = new TempLogDirectory();
        Directory.CreateDirectory(logs.FolderPath);

        // a file where the configuration's parent directory has to be: nothing can be written there
        var blocker = Path.Combine(logs.FolderPath, "blocker");
        File.WriteAllText(blocker, "not a directory");

        var logging = QuillLogging.Create(logs.WriteConfig(), Path.Combine(blocker, "quill.nlog.config"));
        await using var host = await NewLoggingHostAsync(logging);

        var failed = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration,
            new { logs = new { path = logs.FolderPath, minLevel = nameof(LogLevel.Debug) }, persist = true });

        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, failed.StatusCode);

        var audit = await logs.ReadAuditAsync(host, "failed to persist");
        var record = SingleLineContaining(audit, "failed to persist");
        Assert.DoesNotContain("Exception", record);

        // the reason belongs in the normal log, where a stack trace can span lines
        var normal = await logs.ReadNormalAsync(host, "Persisting the log configuration");
        Assert.Contains(blocker, normal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Polly_retry_chatter_is_suppressed()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs);

        var factory = host.Services.GetRequiredService<ILoggerFactory>();
        factory.CreateLogger("Polly").LogWarning("polly-root-must-not-appear");
        factory.CreateLogger("Polly.Retry.Pipeline").LogError("polly-child-must-not-appear");
        factory.CreateLogger("NotPolly").LogWarning("notpolly-must-appear");

        // waiting on the unsuppressed line proves the suppressed ones had their chance to land
        var normal = await logs.ReadNormalAsync(host, "notpolly-must-appear");
        Assert.DoesNotContain("polly-root-must-not-appear", normal);
        Assert.DoesNotContain("polly-child-must-not-appear", normal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Persisted_levels_are_written_to_the_config_file_and_survive_a_restart()
    {
        using var logs = new TempLogDirectory();
        await using var host = await NewLoggingHostAsync(logs, persist: true);

        var response = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration, new
        {
            logs = new { path = logs.FolderPath, minLevel = nameof(LogLevel.Debug) },
            microsoftLogs = new { minLevel = nameof(LogLevel.Warn) },
            persist = true,
        });

        Assert.True(response.IsSuccessStatusCode, await response.Content.ReadAsStringAsync());

        var logging = host.Services.GetRequiredService<QuillLogging>();

        Assert.Equal(LogLevel.Debug, logging.CurrentMinLevel);
        Assert.Equal(LogLevel.Warn, logging.CurrentMicrosoftMinLevel);

        var written = File.ReadAllText(logs.ConfigPath);

        Assert.Contains($"minlevel=\"{nameof(LogLevel.Debug)}\"", written);
        // finalMinLevel goes in one step below the level it means, because NLog blocks what is BELOW it:
        // Warn in the request is Info in the file
        Assert.Contains($"finalMinLevel=\"{nameof(LogLevel.Info)}\"", written);
        Assert.Contains("a comment that must survive a persisted change", written);
        Assert.Contains("maxArchiveDays=\"3\"", written);

        // File.Replace leaves the previous version behind, as JsonConfigFileModifier does
        Assert.Contains($"minlevel=\"{nameof(LogLevel.Info)}\"", File.ReadAllText(logs.ConfigPath + ".bak"));
        Assert.False(File.Exists(logs.ConfigPath + ".tmp"));

        // loading that file again is what a restart is
        var restarted = QuillLogging.Create(logs.ConfigPath);

        Assert.Equal(LogLevel.Debug, restarted.CurrentMinLevel);
        Assert.True(restarted.CurrentMicrosoftMinLevel == LogLevel.Warn,
            $"microsoft level was {restarted.CurrentMicrosoftMinLevel}, file says:\n{written}");
        Assert.Equal(logs.FolderPath, restarted.CurrentLogDirectory);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_persist_request_is_refused_when_no_writable_config_is_configured()
    {
        using var logs = new TempLogDirectory();
        // loaded from the file, but with nowhere to write back to - the image-only case
        await using var host = await NewLoggingHostAsync(logs);

        var logging = host.Services.GetRequiredService<QuillLogging>();

        var refused = await host.Client.PostAsJsonAsync(QuillRoutes.SettingsLogConfiguration, new
        {
            logs = new { path = logs.FolderPath, minLevel = nameof(LogLevel.Debug) },
            persist = true,
        });

        Assert.Equal(System.Net.HttpStatusCode.Conflict, refused.StatusCode);

        // refused before anything is applied, in memory or in the file
        Assert.Equal(LogLevel.Info, logging.CurrentMinLevel);
        Assert.Contains($"minlevel=\"{nameof(LogLevel.Info)}\"", File.ReadAllText(logs.ConfigPath));

        var configuration = await QuillHttp.GetAsync<LogConfigurationResponse>(
            host.Client, QuillRoutes.SettingsLogConfiguration);
        Assert.False(configuration.CanPersist);
    }

    private Task<QuillHost> NewLoggingHostAsync(
        TempLogDirectory logs, bool persist = false, bool ownServer = false) =>
        NewLoggingHostAsync(
            QuillLogging.Create(logs.WriteConfig(), persist ? logs.ConfigPath : null), ownServer);

    private Task<QuillHost> NewLoggingHostAsync(QuillLogging logging, bool ownServer = false)
    {
        void Configure(IServiceCollection services)
        {
            services.RemoveAll<QuillLogging>();
            services.AddSingleton(logging);
        }

        return ownServer
            ? NewHostAsync(configureServices: Configure)
            : QuillHost.CreateAsync(server: null, Host.Config, configureServices: Configure);
    }

    private static string SingleLineContaining(string contents, string needle)
    {
        var matches = contents
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => line.Contains(needle, StringComparison.Ordinal))
            .ToArray();

        Assert.Single(matches);
        return matches[0];
    }

    /// Per-test log directory. Reads share the handle NLog holds, and resolve the sink off the host so
    /// the assertion is made against the instance the endpoints actually wrote through.
    private sealed class TempLogDirectory : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"quill-logs-{Guid.NewGuid():N}");

        public string FolderPath => _path;

        public string ConfigPath => Path.Combine(_path, "quill.nlog.config");

        /// Writes the nlog configuration this host will run on, with both sinks pointed at the temp
        /// directory and wired unless a test wants them off. Same rule and target names the shipped
        /// template uses, since the appliance looks them up by name.
        public string WriteConfig(bool fileSink = true, bool audit = true, LogLevel minLevel = LogLevel.Info,
            bool microsoft = true)
        {
            Directory.CreateDirectory(_path);

            var directory = _path.Replace('\\', '/');
            // hoisted so the interpolation holes stay free of quotes, which a raw string will not take
            var auditWriteTo = audit ? " writeTo=\"QuillLoggingAudit\"" : string.Empty;
            var defaultWriteTo = fileSink ? "AsyncTargetWrapper,Console" : "Console";
            // Fatal is what the contract calls Off: finalMinLevel names the level one step below the one
            // it lets through, so Error here means the API reports Fatal
            var microsoftFinalMinLevel = microsoft ? "Error" : "Fatal";

            // no ${replace-newlines} on the audit layout, as the shipped template has none: a record stays
            // one line because every request-derived value goes through QuillAudit.Safe where it is read
            File.WriteAllText(ConfigPath, $$"""
<?xml version="1.0" encoding="utf-8" ?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <!-- a comment that must survive a persisted change -->
  <variable name="quillLayout" value="${longdate:universalTime=true}|${level:uppercase=true}|${threadid}|${event-properties:item=Resource}|${event-properties:item=Component}|${logger}|${message:withexception=true}|${event-properties:item=Data}" />
  <targets>
    <target xsi:type="AsyncWrapper" name="AsyncTargetWrapper">
      <target name="QuillLogging" xsi:type="File" createDirs="true" fileName="{{directory}}/quill.log"
              header="Date|Level|ThreadID|Resource|Component|Logger|Message|Data"
              layout="${var:quillLayout}" concurrentWrites="false"
              archiveAboveSize="134217728" maxArchiveDays="3" />
    </target>
    <target name="QuillLoggingAudit" xsi:type="File" createDirs="true" fileName="{{directory}}/quill.audit.log"
            header="Date|Level|ThreadID|Resource|Component|Logger|Message|Data"
            layout="${var:quillLayout}" concurrentWrites="false" autoFlush="true"
            archiveAboveSize="134217728" maxArchiveDays="3" />
    <target name="Console" xsi:type="Console" layout="${var:quillLayout}" />
    <target name="Raven_Polly" xsi:type="Null" />
  </targets>
  <rules>
    <logger ruleName="Raven_Polly" name="Polly*" minlevel="Trace" writeTo="Raven_Polly" final="true" />
    <logger ruleName="Raven_System" name="System.*" finalMinLevel="{{microsoftFinalMinLevel}}" />
    <logger ruleName="Raven_Microsoft" name="Microsoft.*" finalMinLevel="{{microsoftFinalMinLevel}}" />
    <logger ruleName="Raven_Default_Audit" name="Audit" levels="Info" final="true"{{auditWriteTo}} />
    <logger ruleName="Raven_Default" name="*" minlevel="{{minLevel}}" writeTo="{{defaultWriteTo}}" />
  </rules>
</nlog>
""");

            return ConfigPath;
        }

        public bool AuditFileExists() => File.Exists(Path.Combine(_path, "quill.audit.log"));

        public string ReadAudit() => Read("quill.audit.log");

        /// For proving something is ABSENT: flushes, then reads, with no wait for content to arrive.
        public string ReadNormalNow() => Read("quill.log");

        /// <summary>
        /// Waits until <paramref name="lastExpected"/> is on disk, then returns the whole file. Log appends
        /// are ordered, so the last line this test caused having landed means every earlier one has —
        /// which is what makes the assertions that follow deterministic instead of racing the writer.
        /// </summary>
        public Task<string> ReadAuditAsync(QuillHost host, string lastExpected) =>
            ReadUntilAsync(host, "quill.audit.log", lastExpected);

        public Task<string> ReadNormalAsync(QuillHost host, string lastExpected) =>
            ReadUntilAsync(host, "quill.log", lastExpected);

        private async Task<string> ReadUntilAsync(QuillHost host, string fileName, string lastExpected)
        {
            var logging = host.Services.GetRequiredService<QuillLogging>();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);

            while (true)
            {
                logging.Flush();
                var contents = Read(fileName);
                if (contents.Contains(lastExpected, StringComparison.Ordinal))
                    return contents;

                if (DateTime.UtcNow > deadline)
                    throw new TimeoutException(
                        $"'{lastExpected}' never reached {fileName}. Contents:{Environment.NewLine}{contents}");

                await Task.Delay(25);
            }
        }

        private string Read(string fileName)
        {
            var file = Path.Combine(_path, fileName);
            if (File.Exists(file) == false)
                return string.Empty;

            // NLog keeps the file open for writing; ask for a shared handle rather than failing
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_path))
                    Directory.Delete(_path, recursive: true);
            }
            catch (IOException)
            {
                // NLog may still hold the handle when the host outlives this scope; a temp dir left
                // behind must not fail an otherwise green test
            }
        }
    }
}
