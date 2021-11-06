using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public enum MessageType : byte
    {
        // Received
        Parse = (byte)'P',
        Bind = (byte)'B',
        Describe = (byte)'D',
        Execute = (byte)'E',
        Sync = (byte)'S',
        Terminate = (byte)'X',
        Query = (byte)'Q',
        Close = (byte)'C',
        Flush = (byte)'H',
        PasswordMessage = (byte)'p',

        // Sent
        ParameterStatus = (byte)'S',
        BackendKeyData = (byte)'K',
        AuthenticationOk = (byte)'R',
        AuthenticationCleartextPassword = (byte)'R',
        ReadyForQuery = (byte)'Z',
        ErrorResponse = (byte)'E',

        ParseComplete = (byte)'1',
        BindComplete = (byte)'2',
        CloseComplete = (byte)'3',
        ParameterDescription = (byte)'t',
        RowDescription = (byte)'T',
        NoData = (byte)'n',
        DataRow = (byte)'D',
        CommandComplete = (byte)'C',
        EmptyQueryResponse = (byte)'I',
    }

    public enum PgObjectType : byte
    {
        PreparedStatement = (byte)'S',
        Portal = (byte)'P'
    }

    /// <remarks>
    /// See <see href="https://www.postgresql.org/docs/current/protocol-error-fields.html"/>
    /// </remarks>
    public enum PgErrorField : byte
    {
        Severity = (byte)'S',
        SeverityNotLocalized = (byte)'V',
        SqlState = (byte)'C',
        Message = (byte)'M',
        Description = (byte)'D',
        Hint = (byte)'H',
        Position = (byte)'P',
        PositionInternal = (byte)'p',
        QueryInternal = (byte)'q',
        Where = (byte)'W',
        SchemaName = (byte)'s',
        TableName = (byte)'t',
        ColumnName = (byte)'c',
        DataTypeName = (byte)'d',
        ConstraintName = (byte)'n',
        FileName = (byte)'F',
        Line = (byte)'L',
        Routine = (byte)'R'
    }

    public class PgColumn
    {
        public string Name;
        /// <summary>
        /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
        /// </summary>
        public int TableObjectId;
        /// <summary>
        /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
        /// </summary>
        public short ColumnIndex;
        public PgType PgType;
        public PgFormat FormatCode;

        public PgColumn(string name, short columnIndex, PgType pgType, PgFormat formatCode, int tableOid = 0)
        {
            Name = name;
            TableObjectId = tableOid;
            ColumnIndex = columnIndex;
            PgType = pgType;
            FormatCode = formatCode;
        }
    }

    public class PgTable
    {
        public List<PgColumn> Columns;
        public List<PgDataRow> Data;

        public PgTable()
        {
            Columns = new List<PgColumn>();
            Data = new List<PgDataRow>();
        }
    }

    public class PgDataRow
    {
        public Memory<ReadOnlyMemory<byte>?> ColumnData;

        public PgDataRow()
        {
            ColumnData = new Memory<ReadOnlyMemory<byte>?>();
        }

        public PgDataRow(Memory<ReadOnlyMemory<byte>?> columnData)
        {
            ColumnData = columnData;
        }
    }
    public enum PgFormat : short
    {
        Text = 0,
        Binary = 1
    }

    public class PgSeverity
    {
        // In ErrorResponse messages
        public const string Error = "ERROR";
        public const string Fatal = "FATAL";
        public const string Panic = "PANIC";

        // In NoticeResponse messages
        public const string Warning = "WARNING";
        public const string Notice = "NOTICE";
        public const string Debug = "DEBUG";
        public const string Info = "INFO";
        public const string Log = "LOG";
    }

    public abstract class Message
    {
        public async Task Init(MessageReader messageReader, PipeReader reader, CancellationToken token)
        {
            var msgLen = await messageReader.ReadInt32Async(reader, token) - sizeof(int);
            var bytesRead = await InitMessage(messageReader, reader, msgLen, token);

            if (msgLen != bytesRead)
            {
                throw new PgFatalException(PgErrorCodes.ProtocolViolation,
                    $"Message is larger than specified in msgLen field, {msgLen} extra bytes in message.");
            }
        }

        public virtual async Task Handle(PgTransaction transaction, MessageBuilder messageBuilder, PipeReader reader, PipeWriter writer, CancellationToken token)
        {
            await HandleMessage(transaction, messageBuilder, writer, token);
        }

        public virtual async Task HandleError(PgErrorException e, PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token)
        {
            // Should assume none of the members are properly initialized
            await writer.WriteAsync(messageBuilder.ErrorResponse(
                PgSeverity.Error,
                e.ErrorCode,
                e.Message,
                e.ToString()), token);
        }

        protected abstract Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token);

        protected abstract Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token);
    }
}
