using System;
using System.IO;

namespace Raven.Client.Documents.Commands.Batches
{
    internal static class PutAttachmentCommandHelper
    {
        public static void ThrowStreamWasAlreadyUsed()
        {
            throw new InvalidOperationException("It is forbidden to re-use the same stream for more than one attachment. Use a unique stream per put attachment command.");
        }

        private static void ThrowPositionNotZero(long streamPosition)
        {
            throw new InvalidOperationException($"Cannot put an attachment with a stream that have position which isn't zero (The position is: {streamPosition}) " +
                                                "since this is most of the time not intended and it is a common mistake.");
        }

        private static void ThrowNotSeekableStream()
        {
            throw new InvalidOperationException(
                "Cannot put an attachment with a not seekable stream. " +
                "We require a seekable stream because we might failover to a different node if the current one is unavailable during the operation.");
        }

        private static void ThrowNotReadableStream()
        {
            throw new InvalidOperationException(
                "Cannot put an attachment with a not readable stream. " +
                "Make sure that the specified stream is readable and was not disposed.");
        }

        public static void PrepareStream(Stream stream)
        {
            stream.Position = 0;
        }

        public static void ValidateStream(Stream stream)
        {
            if (stream.CanRead == false)
                ThrowNotReadableStream();
            if (stream.CanSeek == false)
                ThrowNotSeekableStream();
            if (stream.Position != 0)
                ThrowPositionNotZero(stream.Position);
        }
    }
}
