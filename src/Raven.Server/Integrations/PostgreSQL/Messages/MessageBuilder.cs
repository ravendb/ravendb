using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class MessageBuilder : IDisposable
    {
        private IMemoryOwner<byte> _bufferOwner;
        private Memory<byte> Buffer => _bufferOwner.Memory;

        public MessageBuilder()
        {
            _bufferOwner = MemoryPool<byte>.Shared.Rent(32 * 1024);
        }

        public ReadOnlyMemory<byte> ReadyForQuery(TransactionState transactionState)
        {
            int pos = 0;
            WriteByte((byte)MessageType.ReadyForQuery, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteByte((byte)transactionState, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> EmptyQueryResponse()
        {
            int pos = 0;

            WriteByte((byte)MessageType.EmptyQueryResponse, ref pos);
            WriteInt32(pos + sizeof(int) - sizeof(byte), ref pos);

            return Buffer[..pos];
        }


        public ReadOnlyMemory<byte> AuthenticationOk()
        {
            int pos = 0;
            WriteByte((byte)MessageType.AuthenticationOk, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteInt32(0, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> AuthenticationCleartextPassword()
        {
            int pos = 0;
            WriteByte((byte)MessageType.AuthenticationCleartextPassword, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteInt32(3, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        /// <summary>
        /// Creates an error response message.
        /// </summary>
        /// <param name="severity">A Postgres severity string. See <see cref="PgSeverity"/></param>
        /// <param name="errorCode">A Postgres error code (SqlState). See <see cref="PgErrorCodes"/></param>
        /// <param name="errorMessage">Error message</param>
        /// <param name="description">Error description</param>
        /// <remarks>
        /// More fields that aren't currently supported - https://www.postgresql.org/docs/current/protocol-error-fields.html
        /// </remarks>
        /// <returns>ErrorResponse message</returns>
        public ReadOnlyMemory<byte> ErrorResponse(string severity, string errorCode, string errorMessage, string description = null)
        {
            int pos = 0;
            WriteByte((byte)MessageType.ErrorResponse, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteByte((byte)PgErrorField.Severity, ref pos);
            WriteNullTerminatedString(severity, ref pos);

            WriteByte((byte)PgErrorField.SeverityNotLocalized, ref pos);
            WriteNullTerminatedString(severity, ref pos);

            WriteByte((byte)PgErrorField.SqlState, ref pos);
            WriteNullTerminatedString(errorCode, ref pos);

            WriteByte((byte)PgErrorField.Message, ref pos);
            WriteNullTerminatedString(errorMessage, ref pos);

            if (description != null)
            {
                WriteByte((byte)PgErrorField.Description, ref pos);
                WriteNullTerminatedString(description, ref pos);
            }

            WriteByte(0, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> BackendKeyData(int processId, int sessionId)
        {
            int pos = 0;
            WriteByte((byte)MessageType.BackendKeyData, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteInt32(processId, ref pos);
            WriteInt32(sessionId, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> ParameterStatusMessages(Dictionary<string, string> status)
        {
            int pos = 0;
            foreach (var (key, val) in status)
            {
                ParameterStatus(key, val, ref pos);
            }

            return Buffer[..pos];
        }

        private void ParameterStatus(string key, string value, ref int pos)
        {
            int initialPos = pos;

            WriteByte((byte)MessageType.ParameterStatus, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteNullTerminatedString(key, ref pos);
            WriteNullTerminatedString(value, ref pos);

            // Write length
            WriteInt32(pos - initialPos - sizeof(byte), ref tempPos);
        }

        public ReadOnlyMemory<byte> ParseComplete()
        {
            int pos = 0;

            WriteByte((byte)MessageType.ParseComplete, ref pos);
            WriteInt32(pos + sizeof(int) - sizeof(byte), ref pos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> BindComplete()
        {
            int pos = 0;

            WriteByte((byte)MessageType.BindComplete, ref pos);
            WriteInt32(pos + sizeof(int) - sizeof(byte), ref pos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> CloseComplete()
        {
            int pos = 0;

            WriteByte((byte)MessageType.CloseComplete, ref pos);
            WriteInt32(pos + sizeof(int) - sizeof(byte), ref pos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> CommandComplete(string tag)
        {
            int pos = 0;
            WriteByte((byte)MessageType.CommandComplete, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteNullTerminatedString(tag, ref pos);

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> DataRow(Span<ReadOnlyMemory<byte>?> columns)
        {
            int pos = 0;
            WriteByte((byte)MessageType.DataRow, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            WriteInt16((short)columns.Length, ref pos);

            foreach (var column in columns)
            {
                WriteInt32(column?.Length ?? -1, ref pos);
                WriteBytes(column ?? ReadOnlyMemory<byte>.Empty, ref pos);
            }

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> RowDescription(ICollection<PgColumn> columns)
        {
            int pos = 0;
            WriteByte((byte)MessageType.RowDescription, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            if (!ConvertToShort(columns.Count, out var columnsCount))
            {
                throw new InvalidCastException($"Columns list is too long to be contained in the message ({columnsCount}).");
            }

            WriteInt16(columnsCount, ref pos);

            foreach (var field in columns)
            {
                WriteNullTerminatedString(field.Name, ref pos);
                WriteInt32(field.TableObjectId, ref pos);
                WriteInt16(field.ColumnIndex, ref pos);
                WriteInt32(field.PgType.Oid, ref pos);
                WriteInt16(field.PgType.Size, ref pos);
                WriteInt32(field.PgType.TypeModifier, ref pos);
                WriteInt16((short)field.FormatCode, ref pos);
            }

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> NoData()
        {
            int pos = 0;

            WriteByte((byte)MessageType.NoData, ref pos);
            WriteInt32(pos + sizeof(int) - sizeof(byte), ref pos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> ParameterDescription(IReadOnlyList<int> parametersDataTypeObjectIds)
        {
            int pos = 0;
            WriteByte((byte)MessageType.ParameterDescription, ref pos);

            // Skip length
            int tempPos = pos;
            pos += sizeof(int);

            if (!ConvertToShort(parametersDataTypeObjectIds.Count, out var paramCount))
            {
                throw new InvalidCastException($"Parameter data type list is too long to be contained " +
                                               $"in the message ({paramCount}).");
            }

            WriteInt16(paramCount, ref pos);

            foreach (var t in parametersDataTypeObjectIds)
            {
                WriteInt32(t, ref pos);
            }

            // Write length
            WriteInt32(pos - sizeof(byte), ref tempPos);

            return Buffer[..pos];
        }

        public ReadOnlyMemory<byte> SSLResponse(bool acceptSSL)
        {
            int pos = 0;

            WriteByte(acceptSSL ? (byte)'S' : (byte)'N', ref pos);

            return Buffer[..pos];
        }

        private bool ConvertToShort(int value, out short outVal)
        {
            if (value < short.MinValue || value > short.MaxValue)
            {
                outVal = 0;
                return false;
            }

            outVal = (short)value;
            return true;
        }

        private void WriteBytes(ReadOnlyMemory<byte> value, ref int pos)
        {
            VerifyBufferSize(pos, value.Length);
            value.Span.CopyTo(Buffer.Span[pos..]);
            pos += value.Length;
        }

        private void WriteNullTerminatedString(string value, ref int pos)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            VerifyBufferSize(pos, byteCount);

            pos += Encoding.UTF8.GetBytes(value, Buffer.Span[pos..]);
            WriteByte(0, ref pos);
        }

        private void WriteInt32(int value, ref int pos)
        {
            VerifyBufferSize(pos, sizeof(int));
            var tableObjectIdPayload = MemoryMarshal.Cast<byte, int>(Buffer.Span[pos..]);
            tableObjectIdPayload[0] = IPAddress.HostToNetworkOrder(value);
            pos += sizeof(int);
        }

        private void WriteInt16(short value, ref int pos)
        {
            VerifyBufferSize(pos, sizeof(short));
            var tableObjectIdPayload = MemoryMarshal.Cast<byte, short>(Buffer.Span[pos..]);
            tableObjectIdPayload[0] = IPAddress.HostToNetworkOrder(value);
            pos += sizeof(short);
        }

        private void WriteByte(byte value, ref int pos)
        {
            VerifyBufferSize(pos, sizeof(byte));
            Buffer.Span[pos] = value;
            pos += sizeof(byte);
        }

        private void VerifyBufferSize(int pos, int addedSize)
        {
            if (pos + addedSize > Buffer.Length)
            {
                using (var oldOwner = _bufferOwner)
                {
                    _bufferOwner = MemoryPool<byte>.Shared.Rent(oldOwner.Memory.Length * 2);
                    oldOwner.Memory.CopyTo(_bufferOwner.Memory);
                }
            }
        }

        public void Dispose()
        {
            _bufferOwner?.Dispose();
        }
    }
}
