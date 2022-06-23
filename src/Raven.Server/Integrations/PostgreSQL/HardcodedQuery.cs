using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Npgsql;
using Raven.Server.Integrations.PostgreSQL.PowerBI;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class HardcodedQuery : PgQuery
    {
        private PgTable _result;

        public HardcodedQuery(string queryString, int[] parametersDataTypes, PgTable result) : base(queryString, parametersDataTypes)
        {
            _result = result;
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, PgSession session, out HardcodedQuery hardcodedQuery)
        {
            // TODO: The hardcoded queries in NpgsqlConfig might look a bit different for every user because they are generated using a function. Add support to more than just the current queries by not matching the entire string but ignoring parts of it.
            // TODO: For more accurate implementation, use the `resultsFormat` and send an appropriate _result table (Binary or Text). So for example return PowerBIConfig.TableScheamResponseBinary when the foramt is binary, and PowerBIConfig.TableScheamResponseText otherwise.
            // var resultsFormat = GetDefaultResultsFormat();

            var normalizedQuery = queryText.NormalizeLineEndings();
            PgTable result = null;

            if (normalizedQuery.StartsWith(PowerBIConfig.TableSchemaQuery, StringComparison.OrdinalIgnoreCase))
                result = PowerBIConfig.TableSchemaResponse;

            else if (normalizedQuery.StartsWith(PowerBIConfig.TableSchemaSecondaryQuery, StringComparison.OrdinalIgnoreCase))
                result = PowerBIConfig.TableSchemaSecondaryResponse;

            else if (normalizedQuery.StartsWith(PowerBIConfig.ConstraintsQuery, StringComparison.OrdinalIgnoreCase))
                result = PowerBIConfig.ConstraintsResponse;

            else if (normalizedQuery.Equals(PowerBIConfig.CharacterSetsQuery, StringComparison.OrdinalIgnoreCase))
                result = PowerBIConfig.CharacterSetsResponse;

            // Npgsql
            else if (normalizedQuery.Equals(NpgsqlConfig.TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.CompositeTypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.EnumTypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.EnumTypesResponse;

            else if (normalizedQuery.Replace("\n", "").Equals(NpgsqlConfig.VersionQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.VersionResponse;
                
            else if (normalizedQuery.Equals(NpgsqlConfig.VersionCurrentSettingQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.VersionCurrentSettingResponse;
            
            else if (normalizedQuery.Equals(NpgsqlConfig.CurrentSettingQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.CurrentSettingResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql5TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql5CompositeTypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5EnumTypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql5EnumTypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql4TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_1_2TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql4_1_2TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_3TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql4_0_3TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_0TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql4_0_0TypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_0CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql4_0_0CompositeTypesResponse;

            else if (normalizedQuery.Equals(NpgsqlConfig.Npgsql3TypesQuery, StringComparison.OrdinalIgnoreCase))
                result = NpgsqlConfig.Npgsql3TypesResponse;

            else if (normalizedQuery.StartsWith("DISCARD ALL", StringComparison.OrdinalIgnoreCase))
                result = new PgTable();

            else if (normalizedQuery.StartsWith("ROLLBACK", StringComparison.OrdinalIgnoreCase))
                result = new PgTable();

            else if (normalizedQuery.StartsWith("DEALLOCATE", StringComparison.OrdinalIgnoreCase))
            {
                var statementName = normalizedQuery.Split("\"")[1];
                if (session.NamedStatements.TryRemove(statementName, out var statement) == false)
                    throw new InvalidOperationException($"Failed to remove prepared statement '{statementName}'");
                statement.IsNamedStatement = false;
                statement.Dispose();
                result = new PgTable();
            }
                
            if (result != null)
            {
                hardcodedQuery = new HardcodedQuery(queryText, parametersDataTypes, result);
                return true;
            }

            hardcodedQuery = null;
            return false;
        }

        public override Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            if (IsEmptyQuery)
                return Task.FromResult<ICollection<PgColumn>>(null);

            if (_result != null)
                return Task.FromResult<ICollection<PgColumn>>(_result.Columns);

            return Task.FromResult<ICollection<PgColumn>>(Array.Empty<PgColumn>());
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            if (_result?.Data != null)
            {
                foreach (var dataRow in _result.Data)
                {
                    await writer.WriteAsync(builder.DataRow(dataRow.ColumnData.Span), token);
                }
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {_result?.Data?.Count ?? 0}"), token);
        }

        public override void Dispose()
        {
        }
    }
}
