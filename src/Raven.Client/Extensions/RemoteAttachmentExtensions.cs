using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

namespace Raven.Client.Extensions;

/// <summary>
/// Provides extension methods for working with attachment remote parameters.
/// </summary>
internal static class RemoteAttachmentExtensions
{
    /// <summary>
    /// Determines whether the attachment parameters indicate a local (non-remote) attachment.
    /// </summary>
    /// <param name="parameters">The remote parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are null or have flags set to None (<see cref="RemoteAttachmentFlags.None"/>), indicating a local storage attachment;
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsLocalStorageAttachment(this RemoteAttachmentParameters parameters)
    {
        return parameters == null || parameters.Flags == RemoteAttachmentFlags.None;
    }

    /// <summary>
    /// Determines whether the attachment parameters indicate a remote storage attachment.
    /// </summary>
    /// <param name="parameters">The remote parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are not null and have flags set to Remote (<see cref="RemoteAttachmentFlags.Remote"/>), indicating a remote cloud storage attachment.
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsRemoteStorageAttachment(this RemoteAttachmentParameters parameters)
    {
        if (parameters == null)
        {
            return false;
        }

        return parameters.Flags == RemoteAttachmentFlags.Remote;
    }

    private static RemoteAttachmentParameters GetRemoteAttachmentParameters(string identifier, DateTime? remoteAt, RemoteAttachmentFlags flags)
    {
        RemoteAttachmentParameters remoteParameters = null;
        if (remoteAt.HasValue)
        {
            remoteParameters = new RemoteAttachmentParameters(identifier, remoteAt.Value) { Flags = flags };
        }

        return remoteParameters;
    }

    internal static RemoteAttachmentParameters GetRemoteAttachmentParameters(LazyStringValue identifier, DateTime? remoteAt, RemoteAttachmentFlags flags)
    {
        return GetRemoteAttachmentParameters(identifier.ToString(), remoteAt, flags);
    }
}
