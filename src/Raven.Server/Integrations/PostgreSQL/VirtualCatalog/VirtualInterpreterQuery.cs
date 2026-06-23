using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    // Wraps a PgTable produced by PgVirtualInterpreter and streams it over the wire.
    internal sealed class VirtualInterpreterQuery : PgQuery
    {
        private readonly PgTable _result;

        public VirtualInterpreterQuery(string queryString, int[] parametersDataTypes, PgTable result)
            : base(queryString, parametersDataTypes)
        {
            _result = result;
        }

        public override Task<ICollection<PgColumn>> Init()
        {
            if (IsEmptyQuery)
                return Task.FromResult<ICollection<PgColumn>>(null);

            return Task.FromResult<ICollection<PgColumn>>(_result?.Columns);
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
