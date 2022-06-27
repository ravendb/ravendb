using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced.Extensions;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL
{
    public enum TransactionState : byte
    {
        Idle = (byte)'I',
        InTransaction = (byte)'T',
        Failed = (byte)'E'
    }

    public class PgTransaction : IDisposable
    {
        public TransactionState State { get; private set; } = TransactionState.Idle;
        public DocumentDatabase DocumentDatabase { get; }
        public MessageReader MessageReader { get; private set; }
        public string Username { get; private set; }
        
        internal PgQuery _currentQuery;
        internal PgSession Session { get; init; }
        
        public PgTransaction(DocumentDatabase documentDatabase, MessageReader messageReader, string username, PgSession session)
        {
            DocumentDatabase = documentDatabase;
            MessageReader = messageReader;
            Username = username;
            Session = session;
        }

        public void Init(string cleanQueryText, int[] parametersDataTypes)
        {
            State = TransactionState.InTransaction;

            MessageReader?.Dispose();
            MessageReader = new MessageReader();

            _currentQuery?.Dispose();
            _currentQuery = PgQuery.CreateInstance(cleanQueryText, parametersDataTypes, DocumentDatabase, Session);
        }

        public void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes, string statementName = null)
        {
            if (statementName.IsNullOrWhiteSpace() == false)
            {
                State = TransactionState.InTransaction;
                if (Session.NamedStatements.TryGetValue(statementName, out _currentQuery) == false)
                    throw new KeyNotFoundException($"Expected named statement '{statementName}' wasn't found.");
            }
            _currentQuery.Bind(parameters, parameterFormatCodes, resultColumnFormatCodes);
        }

        public async Task<(ICollection<PgColumn> schema, int[] parameterDataTypes)> Describe()
        {
            return (await _currentQuery.Init(), _currentQuery.ParametersDataTypes);
        }

        public async Task Execute(MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            await _currentQuery.Execute(messageBuilder, writer, token);
        }

        public void Fail()
        {
            State = TransactionState.Failed;
        }

        public void Close()
        {
            State = TransactionState.Idle;

            _currentQuery?.Dispose();
            _currentQuery = null;
        }

        public void Sync()
        {
            State = TransactionState.Idle;
            _currentQuery = null;
            _currentQuery?.Dispose();
        }

        public void Dispose()
        {
            _currentQuery?.Dispose();
            _currentQuery = null;

            MessageReader?.Dispose();
            MessageReader = null;
        }
    }
}
