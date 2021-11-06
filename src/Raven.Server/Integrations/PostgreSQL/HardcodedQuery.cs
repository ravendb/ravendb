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

        public HardcodedQuery(string queryString, int[] parametersDataTypes) : base(queryString, parametersDataTypes)
        {
        }

        public void Parse(bool allowMultipleStatements)
        {
            // TODO: The hardcoded queries in NpgsqlConfig might look a bit different for every user because they are generated using a function. Add support to more than just the current queries by not matching the entire string but ignoring parts of it.
            // TODO: For more accurate implementation, use the `resultsFormat` and send an appropriate _result table (Binary or Text). So for example return PowerBIConfig.TableScheamResponseBinary when the foramt is binary, and PowerBIConfig.TableScheamResponseText otherwise.
            var resultsFormat = GetDefaultResultsFormat();

            var normalizedQuery = QueryString.NormalizeLineEndings();

            // PowerBI
            if (normalizedQuery.StartsWith(PowerBIConfig.TableSchemaQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = PowerBIConfig.TableSchemaResponse;
                return;
            }

            if (normalizedQuery.StartsWith(PowerBIConfig.TableSchemaSecondaryQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = PowerBIConfig.TableSchemaSecondaryResponse;
                return;
            }

            if (normalizedQuery.StartsWith(PowerBIConfig.ConstraintsQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = PowerBIConfig.ConstraintsResponse;
                return;
            }

            if (normalizedQuery.Equals(PowerBIConfig.CharacterSetsQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = PowerBIConfig.CharacterSetsResponse;
                return;
            }

            // Npgsql
            if (normalizedQuery.Equals(NpgsqlConfig.TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.CompositeTypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.EnumTypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.EnumTypesResponse;
                return;
            }

            if (normalizedQuery.Replace("\n", "").Equals(NpgsqlConfig.VersionQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.VersionResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql5TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql5CompositeTypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql5EnumTypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql5EnumTypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql4TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_1_2TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql4_1_2TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_3TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql4_0_3TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_0TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql4_0_0TypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql4_0_0CompositeTypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql4_0_0CompositeTypesResponse;
                return;
            }

            if (normalizedQuery.Equals(NpgsqlConfig.Npgsql3TypesQuery, StringComparison.OrdinalIgnoreCase))
            {
                _result = NpgsqlConfig.Npgsql3TypesResponse;
                return;
            }
        }

        public override Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            if (IsEmptyQuery)
            {
                return Task.FromResult<ICollection<PgColumn>>(null);
            }

            Parse(allowMultipleStatements);

            if (_result != null)
            {
                return Task.FromResult<ICollection<PgColumn>>(_result.Columns);
            }

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
