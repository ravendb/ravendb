using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public class MessageReader : IDisposable
    {
        private readonly List<byte[]> _rentedBuffers = new List<byte[]>();
        public async Task<IInitialMessage> ReadInitialMessage(PipeReader reader, CancellationToken token)
        {
            var msgLen = await ReadInt32Async(reader, token) - sizeof(int);
            var protocolVersion = await ReadInt32Async(reader, token);

            return protocolVersion switch
            {
                (int)ProtocolVersion.CancelMessage => await Cancel(msgLen, reader, token),
                (int)ProtocolVersion.TlsConnection => new SSLRequest(),
                _ => await StartupMessage(protocolVersion, msgLen, reader, token)
            };
        }

        private async Task<StartupMessage> StartupMessage(int version, int msgLen, PipeReader reader, CancellationToken token)
        {
            if (version != (int)ProtocolVersion.Version3)
            {
                throw new PgFatalException(PgErrorCodes.ProtocolViolation, "Unsupported protocol version: " + version);
            }

            msgLen -= sizeof(int);
            var clientOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            while (msgLen > 0)
            {
                var (key, keyLenInBytes) = await ReadNullTerminatedString(reader, token);
                msgLen -= keyLenInBytes;
                if (msgLen == 0)
                    break;
                var (val, valLenInBytes) = await ReadNullTerminatedString(reader, token);
                clientOptions[key] = val;
                msgLen -= valLenInBytes;
            }

            if (clientOptions.TryGetValue("client_encoding", out var encoding) && !encoding.Equals("UTF8", StringComparison.OrdinalIgnoreCase))
                throw new PgFatalException(PgErrorCodes.FeatureNotSupported, "Only UTF8 encoding is supported, but got: " + encoding);

            if (clientOptions.TryGetValue("database", out _) == false)
                throw new PgFatalException(PgErrorCodes.ConnectionException, "The database wasn't specified, but is mandatory");

            return new StartupMessage
            {
                ProtocolVersion = ProtocolVersion.Version3,
                ClientOptions = clientOptions
            };
        }

        private async Task<Cancel> Cancel(int msgLen, PipeReader reader, CancellationToken token)
        {
            // Length field
            msgLen -= sizeof(int);

            var processId = await ReadInt32Async(reader, token);
            msgLen -= sizeof(int);

            var sessionId = await ReadInt32Async(reader, token);
            msgLen -= sizeof(int);

            if (msgLen != 0)
            {
                throw new PgFatalException(PgErrorCodes.ProtocolViolation,
                    $"Message is bigger than specified in msgLen field - {msgLen} extra bytes in message.");
            }

            return new Cancel
            {
                ProcessId = processId,
                SessionId = sessionId
            };
        }

        public async Task<Message> GetUninitializedMessage(PipeReader reader, CancellationToken token)
        {
            var msgType = await ReadByteAsync(reader, token);

            return msgType switch
            {
                (byte)MessageType.Parse => new Parse(),
                (byte)MessageType.Bind => new Bind(),
                (byte)MessageType.Describe => new Describe(),
                (byte)MessageType.Execute => new Execute(),
                (byte)MessageType.Sync => new Sync(),
                (byte)MessageType.Terminate => new Terminate(),
                (byte)MessageType.Query => new Query(),
                (byte)MessageType.Close => new Close(),
                (byte)MessageType.Flush => new Flush(),
                (byte)MessageType.PasswordMessage => new PasswordMessage(),
                _ => throw new PgFatalException(PgErrorCodes.ProtocolViolation,
                    "Message type unrecognized: " + (char)msgType)
            };
        }

        public async Task<(string String, int LengthInBytes)> ReadNullTerminatedString(PipeReader reader, CancellationToken token)
        {
            ReadResult read;
            SequencePosition? end;

            while (true)
            {
                read = await reader.ReadAsync(token);
                end = read.Buffer.PositionOf((byte)0);
                if (end != null)
                    break;

                reader.AdvanceTo(read.Buffer.Start, read.Buffer.End);
            }

            var match = read.Buffer.Slice(0, end.Value);
            var result = Encoding.UTF8.GetString(match);
            reader.AdvanceTo(read.Buffer.GetPosition(1, end.Value));

            return (result, (int)match.Length + 1);
        }

        private async Task<ReadResult> ReadMinimumOf(PipeReader reader, int minSizeRequired, CancellationToken token)
        {
            var read = await reader.ReadAsync(token);

            while (read.Buffer.Length < minSizeRequired)
            {
                reader.AdvanceTo(read.Buffer.Start, read.Buffer.End);
                read = await reader.ReadAsync(token);
                
                if (read.Buffer.Length < minSizeRequired && read.IsCompleted)
                {
                    // we aren't making progress and nothing else will be forthcoming

                    throw new PgErrorException(PgErrorCodes.ProtocolViolation,
                        $"Expected to read at least {minSizeRequired} but got {read.Buffer.Length} and the pipe is already closed");
                }
            }

            return read;
        }

        public async Task<byte[]> ReadBytesAsync(PipeReader reader, int length, CancellationToken token)
        {
            var read = await ReadMinimumOf(reader, length, token);
            return ReadBytes(read.Buffer, reader, length);
        }

        public async Task<int> ReadInt32Async(PipeReader reader, CancellationToken token)
        {
            var read = await ReadMinimumOf(reader, sizeof(int), token);
            return ReadInt32(read.Buffer, reader);
        }

        public async Task<short> ReadInt16Async(PipeReader reader, CancellationToken token)
        {
            var read = await ReadMinimumOf(reader, sizeof(int), token);
            return ReadInt16(read.Buffer, reader);
        }

        public async Task<byte> ReadByteAsync(PipeReader reader, CancellationToken token)
        {
            var read = await ReadMinimumOf(reader, sizeof(byte), token);
            return ReadByte(read.Buffer, reader);
        }

        public async Task SkipBytesAsync(PipeReader reader, int length, CancellationToken token)
        {
            var read = await ReadMinimumOf(reader, length, token);
            SkipBytes(read.Buffer, reader, length);
        }

        private void SkipBytes(ReadOnlySequence<byte> readBuffer, PipeReader reader, int length)
        {
            var sequence = readBuffer.Slice(0, length);
            reader.AdvanceTo(sequence.End);
        }

        private byte[] ReadBytes(ReadOnlySequence<byte> readBuffer, PipeReader reader, int length)
        {
            var sequence = readBuffer.Slice(0, length);

            byte[] buffer;
            if (length < 1 * 1024 * 1024)
            {
                buffer = ArrayPool<byte>.Shared.Rent(length);
                _rentedBuffers.Add(buffer);
            }
            else
            {
                buffer = new byte[length];
            }

            sequence.CopyTo(buffer);
            reader.AdvanceTo(sequence.End);
            return buffer;
        }

        private int ReadInt32(ReadOnlySequence<byte> readBuffer, PipeReader reader)
        {
            var sequence = readBuffer.Slice(0, sizeof(int));
            Span<byte> buffer = stackalloc byte[sizeof(int)];
            sequence.CopyTo(buffer);
            reader.AdvanceTo(sequence.End);
            return IPAddress.NetworkToHostOrder(MemoryMarshal.AsRef<int>(buffer));
        }

        private short ReadInt16(ReadOnlySequence<byte> readBuffer, PipeReader reader)
        {
            var sequence = readBuffer.Slice(0, sizeof(short));
            Span<byte> buffer = stackalloc byte[sizeof(short)];
            sequence.CopyTo(buffer);
            reader.AdvanceTo(sequence.End);
            return IPAddress.NetworkToHostOrder(MemoryMarshal.AsRef<short>(buffer));
        }
        private byte ReadByte(ReadOnlySequence<byte> readBuffer, PipeReader reader)
        {
            var charByte = readBuffer.First.Span[0];
            reader.AdvanceTo(readBuffer.GetPosition(sizeof(byte), readBuffer.Start));
            return charByte;
        }

        public void Dispose()
        {
            foreach (var buffer in _rentedBuffers)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
