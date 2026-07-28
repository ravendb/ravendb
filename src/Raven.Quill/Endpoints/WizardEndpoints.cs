using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Infrastructure;
using Raven.Quill.Wizard;

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
            .WithDescription("Creates the app. The slug (also the app's database name, used in public " +
                             "embed URLs) derives from appName unless an explicit slug is supplied; either " +
                             "is normalized to lowercase ASCII alphanumerics with hyphens. Duplicate slug => 409.")
            .Accepts<ProvisionRequest>("application/json")
            .Produces<ProvisionResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict);
    }

    private static async Task<IResult> ConnectAsync(
        ConnectRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
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
            logger.LogError(ex, "Connect: test-connection threw");
            result = new ConnectResult();
            result.Errors.Add(new WizardError(ex.ToString()));
        }
        
        for (var i = 0; i < result.Errors.Count; i++)
            result.Errors[i] = WizardErrorFormatter.FormatConnectionError(result.Errors[i].Message);

        var wizardId = WizardState.DocumentIdFor(body.Slug);
        using (var session = store.OpenAsyncSession())
        {
            var state = new WizardState
            {
                Provider = factoryName,
                SourceConnectionString = connectionString,
                LastVerifyResult = result,
                LastVerifyAt = DateTime.UtcNow,
            };
            await session.StoreAsync(state, wizardId, ct);
            await session.SaveChangesAsync(ct);
        }

        return Results.Ok(result);
    }

    private static async Task<IResult> DiscoverAsync(
        DiscoverRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
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
            logger.LogError(ex, "Discover: schema enumeration threw");
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

    private static async Task<IResult> MapAsync(
        MapRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
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
            logger.LogInformation("Map: configuration rejected by Validate ({Count} errors)", errors.Count);
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

        return Results.Ok(cdcConfig);
    }

    private static async Task<IResult> SuggestCdcAsync(
        SuggestCdcRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        ILogger<WizardLogger> logger,
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
            logger.LogInformation("SuggestCdc: returned configuration failed validation ({Count} errors)", errors.Count);
            return Results.UnprocessableEntity(new ApiErrorResponse(Errors: errors.ToArray()));
        }

        return Results.Ok(new SuggestCdcResponse(result.Configuration, result.Rationale, result.Status.ToString()));
    }

    private static async Task<IResult> TestMappingAsync(
        TestMappingRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
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
            logger.LogError(ex, "TestMapping: SendAsync threw");
            result = new TestCdcSinkMappingResult();
            result.Errors.Add(ex.ToString());
        }

        return Results.Ok(TestMappingResponse.From(result));
    }

    private static async Task<IResult> ProvisionAsync(
        ProvisionRequest body,
        IDocumentStore store,
        ILogger<WizardLogger> logger,
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

        CdcSinkConfiguration cdcConfig;
        string sourceConnectionString;
        string? sourceProvider;
        using (var session = store.OpenAsyncSession(new Client.Documents.Session.SessionOptions { NoTracking = true }))
        {
            var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), ct);
            if (state?.LastMapConfiguration is null)
                return Results.BadRequest(new ApiErrorResponse("no map configuration found; call /api/setup/map first"));
            if (string.IsNullOrEmpty(state.SourceConnectionString))
                return Results.BadRequest(new ApiErrorResponse("no source connection found; call /api/setup/connect first"));

            cdcConfig = state.LastMapConfiguration;
            sourceConnectionString = state.SourceConnectionString;
            sourceProvider = state.Provider;
        }

        bool created;
        try
        {
            // cluster-wide-atomic: this call IS the slug-uniqueness gate
            created = await RavenStoreFactory.EnsureDatabaseAsync(store, slug, ct: ct);
        }
        catch (ConcurrencyException)
        {
            return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));
        }

        if (!created)
            return Results.Conflict(new ApiErrorResponse(DatabaseExistsMessage(slug)));

        // transplant the source creds captured at connect (held on the wizard doc) onto the app DB
        var transplantedCs = new SqlConnectionString
        {
            Name = cdcConfig.ConnectionStringName,
            FactoryName = sourceProvider,
            ConnectionString = sourceConnectionString,
        };
        await store.Maintenance.ForDatabase(slug).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(transplantedCs), ct);

        cdcConfig.Name = $"{slug}-cdc";
        await store.Maintenance.ForDatabase(slug).SendAsync(new AddCdcSinkOperation(cdcConfig), ct);

        // Shared app-creation: read-model indexes + expiration/revisions + the apps/{slug} doc.
        var app = await AppProvisioner.CreateAppAsync(store, slug, body.AppName, cdcConfig.Name, ct);

        logger.LogInformation("Provisioned app slug={Slug} id={Id} cdcTask={CdcTaskName}",
            app.Slug, app.Id, app.CdcTaskName);

        return Results.Ok(new ProvisionResponse(app.Id!, app.Slug));
    }

    /// <summary>
    /// Narrows the persisted discovery result to the tables the operator selected on the verify step.
    /// Discovery enumerates whole schemas, so handing the AI everything it found would map tables the
    /// operator deliberately left out.
    /// </summary>
    private static CdcSinkSourceSchema SelectTables(CdcSinkSourceSchema schema, SelectedSourceTable[] selectedTables)
    {
        var selectedKeys = selectedTables
            .Select(table => TableKey(table.SourceTableSchema, table.SourceTableName))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

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
    private static CdcSinkSourceTable WithForeignKeysWithin(CdcSinkSourceTable table, HashSet<string> selectedKeys) => new()
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

    private static string TableKey(string? schemaName, string tableName) => $"{schemaName}.{tableName}";

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
