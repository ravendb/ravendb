using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL
{
    public abstract class PgQuery : IDisposable
    {
        protected readonly string QueryString;
        public readonly int[] ParametersDataTypes;
        protected readonly bool IsEmptyQuery;
        public Dictionary<string, object> Parameters;
        protected readonly Dictionary<string, PgColumn> Columns;
        private short[] _resultColumnFormatCodes;

        protected PgQuery(string queryString, int[] parametersDataTypes)
        {
            QueryString = queryString.Trim();
            ParametersDataTypes = parametersDataTypes ?? Array.Empty<int>();
            IsEmptyQuery = string.IsNullOrWhiteSpace(QueryString);
            Parameters = new Dictionary<string, object>();
            Columns = new Dictionary<string, PgColumn>();
            _resultColumnFormatCodes = Array.Empty<short>();
        }

        public static PgQuery CreateInstance(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase)
        {
            queryText = queryText.Trim();

            if (RqlQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out var rqlQuery))
                return rqlQuery;

            if (PowerBIQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out var powerBiQuery))
                return powerBiQuery;

            return new HardcodedQuery(queryText, parametersDataTypes);
        }

        protected PgFormat GetDefaultResultsFormat()
        {
            return _resultColumnFormatCodes.Length switch
            {
                0 => PgFormat.Text,
                1 => _resultColumnFormatCodes[0] == 0 ? PgFormat.Text : PgFormat.Binary,
                _ => throw new NotSupportedException(
                    "No support for column format code count that isn't 0 or 1, got: " +
                    _resultColumnFormatCodes.Length)
            };
        }

        public abstract Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false);

        public abstract Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token);

        public abstract void Dispose();

        public virtual void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes)
        {
            _resultColumnFormatCodes = resultColumnFormatCodes;

            PgFormat? defaultParamDataFormat = parameterFormatCodes.Length switch
            {
                0 => PgFormat.Text,
                1 => parameterFormatCodes[0] == 1 ? PgFormat.Binary : PgFormat.Text,
                _ => (parameters.Count == parameterFormatCodes.Length) ? null :
                     throw new PgErrorException(PgErrorCodes.ProtocolViolation, 
                         $"Got '{parameters.Count}' parameters while given '{parameterFormatCodes.Length}' parameter format codes.")
            };

            for (var i = 0; i < parameters.Count; i++)
            {
                var dataType = i < ParametersDataTypes.Length ? ParametersDataTypes[i] : PgTypeOIDs.Unknown;
                var dataFormat = defaultParamDataFormat ?? (parameterFormatCodes[i] == 1 ? PgFormat.Binary : PgFormat.Text);

                var pgType = PgType.Parse(dataType);
                var processedParameter = pgType.FromBytes(parameters.ElementAt(i), dataFormat);
                Parameters.Add($"p{i + 1}", processedParameter);
            }
        }
    }
}
