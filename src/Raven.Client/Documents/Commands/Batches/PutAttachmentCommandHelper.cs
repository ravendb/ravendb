using System;
using System.IO;

namespace Raven.Client.Documents.Commands.Batches
{
    public static class PutAttachmentCommandHelper
    {
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

        public static void PrepareStream(Stream stream)
        {
            stream.Position = 0;
        }
    }
}