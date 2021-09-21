using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;

namespace Raven.Server.Integrations.PostgreSQL
{
    public enum TransactionState : byte
    {
        Idle = (byte)'I',
        InTransaction = (byte)'T',
        Failed = (byte)'E'
    }

    public class Transaction : IDisposable
    {
        public TransactionState State { get; private set; } = TransactionState.Idle;
        public IDocumentStore DocumentStore { get; }
        public MessageReader MessageReader { get; private set; }
        
        private PgQuery _currentQuery;
        
        public Transaction(IDocumentStore documentStore, MessageReader messageReader)
        {
            DocumentStore = documentStore;
            MessageReader = messageReader;
        }

        public void Init(string cleanQueryText, int[] parametersDataTypes)
        {
            State = TransactionState.InTransaction;

            MessageReader?.Dispose();
            MessageReader = new MessageReader();

            _currentQuery?.Dispose();
            _currentQuery = PgQuery.CreateInstance(cleanQueryText, parametersDataTypes, DocumentStore);
        }

        public void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes)
        {
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

            _currentQuery?.Dispose();
            _currentQuery = null;
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
