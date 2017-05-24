using System;
using System.IO;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class PutAttachmentCommandData : ICommandData
    {
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, long? etag)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            Stream = stream;
            ContentType = contentType;
            Etag = etag;

            if (Stream.CanRead == false)
                ThrowNotReadableStream();
            if (Stream.CanSeek == false)
                ThrowNotSeekableStream();
            if (Stream.Position != 0)
                ThrowPositionNotZero(Stream.Position);
        }

        public static void ThrowPositionNotZero(long streamPosition)
        {
            throw new InvalidOperationException($"Cannot put an attachment with a stream that have position which isn't zero (The position is: {streamPosition}) " +
                                                "since this is most of the time not intended and it is a common mistake.");
        }

        public static void ThrowNotSeekableStream()
        {
            throw new InvalidOperationException(
                "Cannot put an attachment with a not seekable stream. " +
                "We require a seekable stream because we might failover to a different node if the current one is unavailable during the operation.");
        }

        public static void ThrowNotReadableStream()
        {
            throw new InvalidOperationException(
                "Cannot put an attachment with a not readable stream. " +
                "Make sure that the specified stream is readable and was not disposed.");
        }

        public string Id { get; }
        public string Name { get; }
        public Stream Stream { get; }
        public string ContentType { get; }
        public long? Etag { get; }
        public CommandType Type { get; } = CommandType.AttachmentPUT;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ContentType)] = ContentType,
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.ToString(),
            };
        }
    }
}