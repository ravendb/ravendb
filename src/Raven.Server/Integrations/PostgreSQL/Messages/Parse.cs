using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class Parse : ExtendedProtocolMessage
    {
        public string StatementName;
        public string Query;

        /// <summary>
        /// Object ID number of parameter data types specified (can be zero).
        /// </summary>
        /// <remarks>
        /// Note that this is not an indication of the number of parameters that might appear
        /// in the query string, only the number that the frontend wants to prespecify types for.
        /// </remarks>
        public int[] ParametersDataTypes;

        private static readonly Regex ParamRegex = new Regex(@"(?<=(?:\$[0-9]))(?:::(?<type>[A-Za-z0-9]+))?", RegexOptions.Compiled);

        protected override async Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token)
        {
            var len = 0;

            var (statementName, statementLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += statementLength;

            var (query, queryLength) = await messageReader.ReadNullTerminatedString(reader, token);
            len += queryLength;

            var parametersCount = await messageReader.ReadInt16Async(reader, token);
            len += sizeof(short);

            var parameters = new int[parametersCount];
            for (var i = 0; i < parametersCount; i++)
            {
                parameters[i] = await messageReader.ReadInt32Async(reader, token);
                len += sizeof(int);
            }

            StatementName = statementName;
            Query = query;
            ParametersDataTypes = parameters;

            return len;
        }

        protected override async Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            // Extract optional parameter types (e.g. $1::int4)
            var foundParamTypes = new List<string>();
            var cleanQueryText = ParamRegex.Replace(Query, new MatchEvaluator((Match match) =>
            {
                foundParamTypes.Add(match.Groups["type"].Value);
                return "";
            }));

            if (ParametersDataTypes.Length < foundParamTypes.Count)
            {
                var arr = ParametersDataTypes;
                ParametersDataTypes = new int[foundParamTypes.Count];
                arr.CopyTo(ParametersDataTypes.AsSpan());
            }

            for (int i = 0; i < foundParamTypes.Count; i++)
            {
                if (ParametersDataTypes[i] == 0)
                {
                    ParametersDataTypes[i] = PgType.Parse(foundParamTypes[i]).Oid;
                }
            }

            transaction.Init(cleanQueryText, ParametersDataTypes);
            if (!string.IsNullOrEmpty(StatementName))
            {
                transaction._currentQuery.IsNamedStatement = true;
                if (transaction.Session.NamedStatements.TryAdd(StatementName, transaction._currentQuery) == false)
                    throw new ArgumentException($"Failed to store statement under the name '{StatementName}', there is already a statement with such name.");

            }
            await writer.WriteAsync(messageBuilder.ParseComplete(), token);
        }
    }
}
