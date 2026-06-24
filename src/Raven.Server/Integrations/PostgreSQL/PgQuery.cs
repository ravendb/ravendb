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
using Raven.Server.Integrations.PostgreSQL.Translation;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public abstract class PgQuery : IDisposable
    {
        private static RavenLogger _log = RavenLogManager.Instance.GetLoggerForServer<PgQuery>();

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

        public static PgQuery CreateInstance(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, PgSession session, string username = null)
        {
            queryText = queryText.Trim();

            try
            {
                if (ProtocolCommandQuery.TryParse(queryText, parametersDataTypes, session, out var protocolCommand))
                    return protocolCommand;

                if (RqlQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out var rqlQuery))
                    return rqlQuery;

                if (PowerBIQuery.TryParse(queryText, parametersDataTypes, documentDatabase, out var powerBiQuery))
                {
                    if (documentDatabase.ServerStore.LicenseManager.CanUsePowerBi(withNotification: true, out var licenseLimitException) == false)
                        throw licenseLimitException;
                    return powerBiQuery;
                }

                if (PgVirtualInterpreter.TryExecute(queryText, new VirtualQueryContext { Database = documentDatabase, Username = username }, out var virtualTable))
                    return new VirtualInterpreterQuery(queryText, parametersDataTypes, virtualTable);

                if (PgSqlToRqlTranslator.TryParse(queryText, parametersDataTypes, documentDatabase, out var rql, out var hasExplicitProjection))
                {
                    return hasExplicitProjection
                        ? new PgSqlTranslatedRqlQuery(rql, parametersDataTypes, documentDatabase)
                        : new RqlQuery(rql, parametersDataTypes, documentDatabase);
                }

                // Targeted FeatureNotSupported errors for known unsupported shapes (JOIN,
                // scalar aggregate, etc.) before the generic StatementTooComplex fallback —
                // gives clients actionable workaround hints instead of an SQL dump.
                if (UnhandledQueryDiagnoser.TryDiagnose(queryText, out var diagnosis))
                {
                    throw new PgErrorException(
                        PgErrorCodes.FeatureNotSupported,
                        $"{diagnosis}{Environment.NewLine}{Environment.NewLine}Query:{Environment.NewLine}{queryText}");
                }

                throw new PgErrorException(
                    PgErrorCodes.StatementTooComplex,
                    $"Unhandled query:{Environment.NewLine}{queryText}");
            }
            catch (InsufficientExecutionStackException)
            {
                throw new PgErrorException(PgErrorCodes.StatementTooComplex, $"Query is too deeply nested to evaluate:{Environment.NewLine}{queryText}");
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to create instance of {nameof(PgQuery)}:{Environment.NewLine}{queryText}", e);

                throw;
            }
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

        public abstract Task<ICollection<PgColumn>> Init();

        public abstract Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token);

        public abstract void Dispose();

        public virtual void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes)
        {
            _resultColumnFormatCodes = resultColumnFormatCodes;

            // A cached prepared statement is re-bound on the same instance, so reset before re-populating.
            Parameters.Clear();

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
                Parameters.Add((i + 1).ToString(), processedParameter);
            }
        }
    }
}
