using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Quill.Contracts;

namespace Raven.Quill.Endpoints;

internal static class SqlConnectionStringValidation
{
    private static readonly Dictionary<string, string> ProviderAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Microsoft.Data.SqlClient"] = "Microsoft.Data.SqlClient",
        ["MySql.Data.MySqlClient"] = "MySql.Data.MySqlClient",
        ["MySqlConnector.MySqlConnectorFactory"] = "MySqlConnector.MySqlConnectorFactory",
        ["MySqlConnectorFactory"] = "MySqlConnector.MySqlConnectorFactory",
        ["Npgsql"] = "Npgsql",
        ["SqlClient"] = "Microsoft.Data.SqlClient",
        ["System.Data.SqlClient"] = "System.Data.SqlClient",
    };

    private static readonly HashSet<SqlProvider> CdcSupportedProviders =
    [
        SqlProvider.Npgsql,
        SqlProvider.SqlClient,
        SqlProvider.MySqlConnectorFactory,
    ];

    public static bool TryNormalizeCdcProvider(string provider, out string factoryName, out ApiErrorResponse error)
    {
        factoryName = string.Empty;

        if (string.IsNullOrWhiteSpace(provider))
        {
            error = new ApiErrorResponse("provider is required");
            return false;
        }

        factoryName = ProviderAliases.GetValueOrDefault(provider.Trim(), provider.Trim());

        SqlProvider parsed;
        try
        {
            parsed = SqlProviderParser.GetSupportedProvider(factoryName);
        }
        catch (Exception ex) when (ex is NotSupportedException or NotImplementedException)
        {
            error = new ApiErrorResponse($"unsupported provider '{provider}': {ex.Message}");
            return false;
        }

        if (!CdcSupportedProviders.Contains(parsed))
        {
            error = new ApiErrorResponse(
                $"provider '{provider}' (parses as {parsed}) is recognized by Raven.Client but not supported by CDC. Supported: {string.Join(", ", CdcSupportedProviders)}.");
            return false;
        }

        error = default!;
        return true;
    }
}
