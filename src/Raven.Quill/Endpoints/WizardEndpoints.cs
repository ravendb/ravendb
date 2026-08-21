using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Infrastructure;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;
using Raven.Server.Logging;

namespace Raven.Quill.Endpoints;

public static class WizardEndpoints
{
    // the source connection string's name on the app DB once provisioned (used when the map didn't set one)
    private const string SourceConnectionStringName = "wizard-source";

    private const string DefaultIntentPrompt =
        "Propose a sensible RavenDB CDC document model from the discovered relational schema: " +
        "map each root/aggregate table to its own collection, embed parent-owned child rows " +
        "(1:N ownership) as nested arrays, and keep many-to-many or shared references as separate " +
        "collections linked by id. Use idiomatic collection names derived from the table names; " +
        "prefer a minimal, query-friendly shape over a literal table-per-collection mirror.";

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/setup").WithTags("setup").RequireAuthorization();
        group.MapPost("/connect", ConnectAsync)
            .WithName("setup.connect")
            .Accepts<ConnectRequest>("application/json")
            .Produces<ConnectResult>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/discover", DiscoverAsync)
            .WithName("setup.discover")
            .Accepts<DiscoverRequest>("application/json")
            .Produces<DiscoverResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/verify-cdc", VerifyCdcAsync)
            .WithName("setup.verifyCdc")
            .WithDescription(
                "Dry-runs CDC against the discovered source: provisions the capture infrastructure " +
                "(PostgreSQL publication/replication slot, SQL Server sp_cdc_enable_*), reads one row per " +
                "selected table, then removes whatever it created. Reports blockers the static schema " +
                "verification cannot see, such as a missing CREATE or REPLICATION grant.")
            .Accepts<VerifyCdcRequest>("application/json")
            .Produces<VerifyCdcResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/map", MapAsync)
            .WithName("setup.map")
            .Accepts<MapRequest>("application/json")
            .Produces<CdcSinkConfiguration>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/suggest/cdc", SuggestCdcAsync)
            .WithName("setup.suggestCdc")
            .Accepts<SuggestCdcRequest>("application/json")
            .Produces<SuggestCdcResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status422UnprocessableEntity);
        group.MapPost("/test-mapping", TestMappingAsync)
            .WithName("setup.testMapping")
            .Accepts<TestMappingRequest>("application/json")
            .Produces<TestMappingResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
        group.MapPost("/provision", ProvisionAsync)
            .WithName("setup.provision")
            .WithDescription("Creates the app, or updates it when one already exists under the same slug. " +
                             "The slug (also the app's database name, used in public embed URLs) derives " +
                             "from appName unless an explicit slug is supplied; either is normalized to " +
                             "lowercase ASCII alphanumerics with hyphens. A slug whose database exists " +
                             "without an app behind it => 409.")
            .Accepts<ProvisionRequest>("application/json")
            .Produces<ProvisionResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict);
    }

    private static async Task<IResult> ConnectAsync(
        ConnectRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body?.Provider, body?.ConnectionString, body?.Slug, out var factoryName, out var error))
            return error;

        var connectionString = body!.ConnectionString;

        ConnectResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new TestSqlConnectionOperation(factoryName, connectionString), ct);
        }
        catch (Exception ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "Connect: test-connection threw");
            result = new ConnectResult();
            result.Errors.Add(new WizardError(ex.ToString()));
        }
        
        for (var i = 0; i < result.Errors.Count; i++)
            result.Errors[i] = WizardErrorFormatter.FormatConnectionError(result.Errors[i].Message);

        var wizardId = WizardState.DocumentIdFor(body.Slug);
        using (var session = store.OpenAsyncSession())
        {
            var state = await session.LoadAsync<WizardState>(wizardId, ct) ?? new WizardState();

            // Re-testing the same source must keep the discovery and mapping already made against it:
            // the wizard skips those calls when its inputs did not change and expects them to still be
            // here. A different source, on the other hand, invalidates both.
            var isSameSource = state.Provider == factoryName && state.SourceConnectionString == connectionString;
            if (!isSameSource)
            {
                state.LastDiscoveredSchema = null;
                state.LastDiscoverAt = null;
                state.LastMapConfiguration = null;
                state.LastMapAt = null;
            }

            state.Provider = factoryName;
            state.SourceConnectionString = connectionString;
            state.LastVerifyResult = result;
            state.LastVerifyAt = DateTime.UtcNow;

            await session.StoreAsync(state, wizardId, ct);
            await session.SaveChangesAsync(ct);
        }

        if (logger.AuditEnabled)
            logger.Audit("POST",
                $"WizardSource '{body.Slug}' provider={factoryName} credential stored", ctx);

        return Results.Ok(result);
    }

    private static async Task<IResult> DiscoverAsync(
        DiscoverRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (TryRejectInvalidRequest(body?.Provider, body?.ConnectionString, body?.Slug, out var factoryName, out var error))
            return error;

        var sqlConnectionString = new SqlConnectionString
        {
            Name = SourceConnectionStringName,
            FactoryName = factoryName,
            ConnectionString = body!.ConnectionString,
        };

        var schemas = body.Schemas?
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var wizardId = WizardState.DocumentIdFor(body.Slug);
        using var session = store.OpenAsyncSession();

        // fail fast before the live schema enumeration if the wizard was never started for this app
        var state = await session.LoadAsync<WizardState>(wizardId, ct);
        if (state is null)
            return Results.BadRequest(new ApiErrorResponse($"no slug {body.Slug} found"));

        CdcSinkSourceSchema schema;
        try
        {
            schema = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new GetCdcSinkSchemaOperation(sqlConnectionString, schemas is { Length: > 0 } ? schemas : null), ct);
        }
        catch (Exception ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "Discover: schema enumeration threw");
            schema = new CdcSinkSourceSchema();
            schema.Errors.Add(ex.ToString());
        }

        state.Provider = factoryName;
        state.LastDiscoveredSchema = schema;
        state.LastDiscoverAt = DateTime.UtcNow;
        await session.StoreAsync(state, wizardId, ct);
        await session.SaveChangesAsync(ct);

        return Results.Ok(DiscoverResponse.From(schema));
    }

    private static async Task<IResult> VerifyCdcAsync(
        VerifyCdcRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body.Tables is not { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("at least one table is required"));
        if (string.IsNullOrWhiteSpace(body.Slug))
            return Results.BadRequest(new ApiErrorResponse("slug is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(body.Slug), ct);

        if (state?.LastDiscoveredSchema is null)
            return Results.BadRequest(new ApiErrorResponse("no discovered schema found; call /api/setup/discover first"));

        if (string.IsNullOrEmpty(state.SourceConnectionString))
            return Results.BadRequest(new ApiErrorResponse("no source connection found; call /api/setup/connect first"));

        var configuration = ScaffoldDryRunConfiguration(state.LastDiscoveredSchema, body.Tables, out var scaffoldErrors);
        if (scaffoldErrors.Count > 0)
        {
            if (logger.IsInfoEnabled)
                logger.Info($"VerifyCdc: {scaffoldErrors.Count} selected table(s) cannot be captured");
            return Results.Ok(VerifyCdcResponse.Failed([.. scaffoldErrors]));
        }

        CdcTestResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new VerifyCdcSinkOperation(new CdcTestRequest
                {
                    Configuration = configuration,
                    // inline source creds off the wizard doc - no probe connection string exists on the config DB
                    Connection = new SqlConnectionString
                    {
                        Name = SourceConnectionStringName,
                        FactoryName = state.Provider,
                        ConnectionString = state.SourceConnectionString,
                    },
                }), ct);
        }
        catch (Exception ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "VerifyCdc: dry run threw");
            result = new CdcTestResult { Success = false, Error = ex.ToString() };
        }

        if (logger.IsInfoEnabled)
            logger.Info(
                $"VerifyCdc: success={result.Success} " +
                $"completedTables={result.CompletedTables.Count}/{configuration.Tables.Count} " +
                $"warnings={result.Warnings.Count}");

        return Results.Ok(VerifyCdcResponse.From(result));
    }

    private static CdcSinkConfiguration ScaffoldDryRunConfiguration(
        CdcSinkSourceSchema schema,
        VerifyCdcTableRequest[] requested,
        out List<string> errors)
    {
        errors = [];

        var configuration = new CdcSinkConfiguration
        {
            Name = "wizard-cdc-dry-run",
            ConnectionStringName = SourceConnectionStringName,
        };

        var collectionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in requested.DistinctBy(
                     t => $"{t.SourceTableSchema ?? string.Empty}.{t.SourceTableName}", StringComparer.OrdinalIgnoreCase))
        {
            var fullName = string.IsNullOrEmpty(table.SourceTableSchema)
                ? table.SourceTableName
                : $"{table.SourceTableSchema}.{table.SourceTableName}";

            var discovered = schema.Tables.FirstOrDefault(t =>
                string.Equals(t.SourceTableName, table.SourceTableName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(t.SourceTableSchema ?? string.Empty, table.SourceTableSchema ?? string.Empty, StringComparison.OrdinalIgnoreCase));

            if (discovered is null)
            {
                errors.Add($"Table '{fullName}' is not part of the discovered schema; re-run discovery and select it again.");
                continue;
            }

            if (!string.IsNullOrWhiteSpace(discovered.UnsupportedReason))
            {
                errors.Add($"Table '{fullName}' cannot be captured: {discovered.UnsupportedReason}");
                continue;
            }

            if (discovered.PrimaryKeyColumns.Count == 0)
            {
                errors.Add($"Table '{fullName}' has no primary key, so CDC cannot derive stable document ids for it.");
                continue;
            }

            var isPendingCdcSetup = schema.HasPermissionToSetup && discovered.IsCdcEnabled == false;

            var columns = discovered.Columns
                .Where(c => c.IsCdcCapturable || isPendingCdcSetup)
                .Select(c => new CdcColumnMapping { Column = c.Name, Name = c.Name, Type = c.SuggestedType })
                .ToList();

            if (columns.Count == 0)
            {
                errors.Add($"Table '{fullName}' cannot be captured: none of its columns can be captured by CDC.");
                continue;
            }

            var uncapturableKeyColumns = discovered.PrimaryKeyColumns
                .Where(pk => !columns.Any(c => string.Equals(c.Column, pk, StringComparison.OrdinalIgnoreCase)))
                .ToArray();

            if (uncapturableKeyColumns.Length > 0)
            {
                errors.Add(
                    $"Table '{fullName}' cannot be captured: primary key column(s) " +
                    $"{string.Join(", ", uncapturableKeyColumns)} are not capturable.");
                continue;
            }

            // schema-qualify only on a clash, i.e. same table name in two selected schemas
            var collectionName = collectionNames.Add(discovered.SourceTableName) ? discovered.SourceTableName : fullName;
            collectionNames.Add(collectionName);

            configuration.Tables.Add(new CdcSinkTableConfig
            {
                CollectionName = collectionName,
                SourceTableSchema = discovered.SourceTableSchema,
                SourceTableName = discovered.SourceTableName,
                Columns = columns,
                PrimaryKeyColumns = [.. discovered.PrimaryKeyColumns],
            });
        }

        return configuration;
    }

    private static async Task<IResult> MapAsync(
        MapRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body required"));
        if (string.IsNullOrWhiteSpace(body.Slug))
            return Results.BadRequest(new ApiErrorResponse("slug is required"));

        var cdcConfig = body.ToClientConfiguration();

        if (string.IsNullOrWhiteSpace(cdcConfig.Name))
            cdcConfig.Name = "wizard-cdc";
        if (string.IsNullOrWhiteSpace(cdcConfig.ConnectionStringName))
            cdcConfig.ConnectionStringName = SourceConnectionStringName;

        if (!cdcConfig.Validate(out var errors, validateName: true, validateConnection: false))
        {
            if (logger.IsInfoEnabled)
                logger.Info($"Map: configuration rejected by Validate ({errors.Count} errors)");
            return Results.BadRequest(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        var wizardId = WizardState.DocumentIdFor(body.Slug);
        using (var session = store.OpenAsyncSession())
        {
            var state = await session.LoadAsync<WizardState>(wizardId, ct);
            if (state is null)
                return Results.BadRequest(new ApiErrorResponse($"no slug {body.Slug} found"));
            state.LastMapConfiguration = cdcConfig;
            state.LastMapAt = DateTime.UtcNow;
            await session.StoreAsync(state, wizardId, ct);
            await session.SaveChangesAsync(ct);
        }

        if (logger.AuditEnabled)
            logger.Audit("POST",
                $"WizardMapping '{body.Slug}' stored (cdcTask='{cdcConfig.Name}' tables={cdcConfig.Tables.Count})",
                ctx);

        return Results.Ok(cdcConfig);
    }

    private static async Task<IResult> SuggestCdcAsync(
        SuggestCdcRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        QuillLogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));
        if (string.IsNullOrWhiteSpace(body.Slug))
            return Results.BadRequest(new ApiErrorResponse("slug is required"));

        if (body.SelectedTables is not { Length: > 0 })
            return Results.BadRequest(new ApiErrorResponse("selectedTables must list at least one table"));

        var intentPrompt = string.IsNullOrWhiteSpace(body.IntentPrompt) ? DefaultIntentPrompt : body.IntentPrompt!;

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(body.Slug), ct);

        if (state?.LastDiscoveredSchema is null)
            return Results.BadRequest(new ApiErrorResponse("no discovered schema found; call /api/setup/discover first"));

        var selectedSchema = SelectTables(state.LastDiscoveredSchema, body.SelectedTables);
        if (selectedSchema.Tables.Count == 0)
            return Results.BadRequest(new ApiErrorResponse(
                "none of the selected tables are part of the discovered schema; call /api/setup/discover first"));

        var result = await aiClient.SuggestCdcAsync(selectedSchema, samples: null, intentPrompt, ct);

        if (result.Status != AiHelperStatus.Success)
            return Results.Ok(new SuggestCdcResponse(Configuration: null, result.Rationale, result.Status.ToString()));

        if (result.Configuration is null)
            return Results.UnprocessableEntity(new ApiErrorResponse("AI service returned a success status but no configuration"));

        if (!result.Configuration.Validate(out var errors, validateName: false, validateConnection: false))
        {
            if (logger.IsInfoEnabled)
                logger.Info($"SuggestCdc: returned configuration failed validation ({errors.Count} errors)");
            return Results.UnprocessableEntity(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        ValidateJoinColumnsAgainstSchema(result.Configuration, state.LastDiscoveredSchema, errors);
        if (errors.Count > 0)
        {
            if (logger.IsInfoEnabled)
                logger.Info(
                    $"SuggestCdc: returned configuration has {errors.Count} join column(s) the source " +
                    "schema does not have");
            return Results.UnprocessableEntity(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        return Results.Ok(new SuggestCdcResponse(result.Configuration, result.Rationale, result.Status.ToString()));
    }

    private static async Task<IResult> TestMappingAsync(
        TestMappingRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.SourceTableName))
            return Results.BadRequest(new ApiErrorResponse("sourceTableName is required"));
        if (string.IsNullOrWhiteSpace(body.Slug))
            return Results.BadRequest(new ApiErrorResponse("slug is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(body.Slug), ct);

        if (state?.LastMapConfiguration is null)
            return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));

        var request = new TestCdcSinkMappingRequest
        {
            Configuration = state.LastMapConfiguration,
            // inline source creds off the wizard doc — no probe connection string exists on the config DB
            Connection = new SqlConnectionString
            {
                Name = SourceConnectionStringName,
                FactoryName = state.Provider,
                ConnectionString = state.SourceConnectionString,
            },
            SourceTableSchema = body.SourceTableSchema,
            SourceTableName = body.SourceTableName,
            RowSelector = TestCdcSinkRowSelector.First,
            Operation = TestCdcSinkOperation.Upsert,
            MaxRows = body.MaxRows ?? 50,
        };

        TestCdcSinkMappingResult result;
        try
        {
            result = await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new TestCdcSinkMappingOperation(request), ct);
        }
        catch (Exception ex)
        {
            if (logger.IsErrorEnabled)
                logger.Error(ex, "TestMapping: SendAsync threw");
            result = new TestCdcSinkMappingResult();
            result.Errors.Add(ex.ToString());
        }

        return Results.Ok(TestMappingResponse.From(result));
    }

    private static async Task<IResult> ProvisionAsync(
        ProvisionRequest body,
        IDocumentStore store,
        QuillLogger<WizardLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.AppName))
            return Results.BadRequest(new ApiErrorResponse("appName is required"));

        var hasSlugOverride = string.IsNullOrWhiteSpace(body.Slug) == false;
        var slug = Slugifier.ToSlug(hasSlugOverride ? body.Slug : body.AppName);
        if (string.IsNullOrEmpty(slug))
            return Results.BadRequest(new ApiErrorResponse(hasSlugOverride
                ? $"slug '{body.Slug}' has no ASCII alphanumeric characters; cannot derive slug."
                : $"appName '{body.AppName}' has no ASCII alphanumeric characters; cannot derive slug."));

        if (slug.Length > Slugifier.MaxLength)
            return Results.BadRequest(new ApiErrorResponse(
                $"slug '{slug}' exceeds the maximum length of {Slugifier.MaxLength} characters"));

        if (string.Equals(slug, store.Database, StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new ApiErrorResponse($"slug '{slug}' is reserved"));

        WizardState state;
        using (var session = store.OpenAsyncSession())
        {
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), ct);
            if (state?.LastMapConfiguration is null)
                return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));
            if (string.IsNullOrEmpty(state.SourceConnectionString))
                return Results.BadRequest(new ApiErrorResponse("no source connection found; call /api/setup/connect first"));
        }

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
        {
            RavenStoreFactory.DatabaseCreationStatus status;
            try
            {
                status = await RavenStoreFactory.EnsureDatabaseAsync(store, slug, ct: ct);
            }
            catch (ConcurrencyException)
            {
                return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));
            }

            if (status.Created == false)
                return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));

            // transplant the source creds captured at connect (held on the wizard doc) onto the app DB
            var transplantedCs = new SqlConnectionString
            {
                Name = state.LastMapConfiguration.ConnectionStringName,
                FactoryName = state.Provider,
                ConnectionString = state.SourceConnectionString,
            };
            await store.Maintenance.ForDatabase(slug).SendAsync(
                new PutConnectionStringOperation<SqlConnectionString>(transplantedCs), ct);

            await AppDatabaseFeatures.ConfigureAsync(store, slug, ct);

            app = new App
            {
                Slug = slug,
                TopologyId = status.DatabaseTopologyId,
                Database = slug,
                CdcTaskName = state.LastMapConfiguration.Name,
                CreatedAt = DateTime.UtcNow,
            };
        }

        using (var session = store.OpenAsyncSession())
        {
            app.AppName = body.AppName;
            await session.StoreAsync(app, id: $"apps/{slug}", ct);
            await session.SaveChangesAsync(ct);
        }

        await CreateOrUpdateCdcAsync(store, app, state.LastMapConfiguration, ct);
        if (logger.IsInfoEnabled)
            logger.Info(
                $"Provisioned app slug={app.Slug} id={app.Id} cdcTask={app.CdcTaskName}");

        if (logger.AuditEnabled)
            logger.Audit("POST",
                $"App '{app.Slug}' provisioned (database={app.Database} cdcTask='{app.CdcTaskName}')", ctx);

        return Results.Ok(new ProvisionResponse(app.Id!, app.Slug));
    }

    private static async Task CreateOrUpdateCdcAsync(IDocumentStore store, App app, CdcSinkConfiguration cdcConfig, CancellationToken ct)
    {
        var slug = app.Slug;
        var existing = await store.Maintenance.ForDatabase(slug).SendAsync(new GetOngoingTaskInfoOperation(app.CdcTaskName, OngoingTaskType.CdcSink), ct);
        if (existing is OngoingTaskCdcSink cdc)
        {
            await store.Maintenance.ForDatabase(slug).SendAsync(new UpdateCdcSinkOperation(cdc.TaskId, cdcConfig), ct);
        }
        else
        {
            cdcConfig.Name = app.CdcTaskName;
            await store.Maintenance.ForDatabase(slug).SendAsync(new AddCdcSinkOperation(cdcConfig), ct);
        }
    }

    /// <summary>
    /// Narrows the persisted discovery result to the tables the operator selected on the verify step.
    /// Discovery enumerates whole schemas, so handing the AI everything it found would map tables the
    /// operator deliberately left out.
    /// </summary>
    private static void ValidateJoinColumnsAgainstSchema(CdcSinkConfiguration configuration, CdcSinkSourceSchema schema, List<string> errors)
    {
        foreach (var table in configuration.Tables)
            ValidateScope(table.SourceTableSchema, table.SourceTableName, table.CollectionName, table.EmbeddedTables, table.LinkedTables);

        void ValidateScope(
            string? tableSchema,
            string tableName,
            string description,
            List<CdcSinkEmbeddedTableConfig>? embeddedTables,
            List<CdcSinkLinkedTableConfig>? linkedTables)
        {
            var columns = SourceColumnsOf(tableSchema, tableName);

            foreach (var linked in linkedTables ?? [])
                CheckJoinColumns(linked.JoinColumns, columns, $"Linked table '{linked.SourceTableName}' under '{description}'");

            foreach (var embedded in embeddedTables ?? [])
            {
                ValidateScope(embedded.SourceTableSchema, embedded.SourceTableName,
                    $"{description}.{embedded.PropertyName}", embedded.EmbeddedTables, embedded.LinkedTables);

                CheckJoinColumns(embedded.JoinColumns, SourceColumnsOf(embedded.SourceTableSchema, embedded.SourceTableName),
                    $"Embedded table '{embedded.SourceTableName}' under '{description}'");
            }
        }

        HashSet<string>? SourceColumnsOf(string? tableSchema, string tableName) => schema.Tables
            .FirstOrDefault(table =>
                string.Equals(table.SourceTableName, tableName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(table.SourceTableSchema ?? string.Empty, tableSchema ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            ?.Columns.Select(column => column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        void CheckJoinColumns(List<string>? joinColumns, HashSet<string>? sourceColumns, string description)
        {
            if (sourceColumns is null)
                return;

            foreach (var joinColumn in joinColumns ?? [])
            {
                if (string.IsNullOrWhiteSpace(joinColumn) || sourceColumns.Contains(joinColumn))
                    continue;

                errors.Add($"{description}: join column '{joinColumn}' is not a column of the source table. " +
                    $"Its columns are: {string.Join(", ", sourceColumns)}.");
            }
        }
    }

    private static CdcSinkSourceSchema SelectTables(CdcSinkSourceSchema schema, SelectedSourceTable[] selectedTables)
    {
        var selectedKeys = selectedTables
            .Select(table => TableKey(table.SourceTableSchema, table.SourceTableName))
            .ToHashSet();

        return new CdcSinkSourceSchema
        {
            CatalogName = schema.CatalogName,
            Tables = schema.Tables
                .Where(table => selectedKeys.Contains(TableKey(table.SourceTableSchema, table.SourceTableName)))
                .Select(table => WithForeignKeysWithin(table, selectedKeys))
                .ToList(),
            Errors = [.. schema.Errors],
            HasPermissionToSetup = schema.HasPermissionToSetup,
            Warnings = [.. schema.Warnings],
        };
    }

    // A foreign key pointing at a left-out table would still invite a linked/embedded mapping onto it.
    private static CdcSinkSourceTable WithForeignKeysWithin(
        CdcSinkSourceTable table,
        HashSet<SourceTableKey> selectedKeys) => new()
    {
        SourceTableSchema = table.SourceTableSchema,
        SourceTableName = table.SourceTableName,
        Columns = table.Columns,
        PrimaryKeyColumns = table.PrimaryKeyColumns,
        ForeignKeys = table.ForeignKeys
            .Where(foreignKey => selectedKeys.Contains(TableKey(foreignKey.ReferencedSchema, foreignKey.ReferencedTable)))
            .ToList(),
        IsCdcEnabled = table.IsCdcEnabled,
        UnsupportedReason = table.UnsupportedReason,
        Warnings = table.Warnings,
    };

    private static SourceTableKey TableKey(string? schemaName, string tableName) => new(schemaName, tableName);

    private readonly record struct SourceTableKey(string? SchemaName, string TableName);

    private static string DatabaseExistsMessage(string slug) =>
        $"database '{slug}' already exists; delete it in RavenDB Studio (or choose another name) and run the setup wizard again";

    private static bool TryRejectInvalidRequest(string? provider, string? connectionString, string? slug, out string factoryName, out IResult error)
    {
        factoryName = string.Empty;

        if (string.IsNullOrWhiteSpace(provider) || string.IsNullOrWhiteSpace(connectionString))
        {
            error = Results.BadRequest(new ApiErrorResponse("provider and connectionString are required"));
            return true;
        }

        if (string.IsNullOrWhiteSpace(slug))
        {
            error = Results.BadRequest(new ApiErrorResponse("slug is required"));
            return true;
        }

        if (!SqlConnectionStringValidation.TryNormalizeCdcProvider(provider, out factoryName, out var providerError))
        {
            error = Results.BadRequest(providerError);
            return true;
        }

        error = default!;
        return false;
    }

    internal sealed class WizardLogger;
}
