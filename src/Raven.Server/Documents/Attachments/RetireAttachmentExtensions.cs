using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

namespace Raven.Server.Documents.Attachments;

/// <summary>
/// Provides extension methods for working with attachment retirement parameters.
/// </summary>
public static class RetireAttachmentExtensions
{
    /// <summary>
    /// Determines whether the attachment parameters indicate a local (non-retired) attachment.
    /// </summary>
    /// <param name="parameters">The retirement parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are null or have flags set to None, indicating a local storage attachment;
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsLocalStorageAttachment(this RetireAttachmentParameters parameters)
    {
        return parameters == null || parameters.Flags == RetiredAttachmentFlags.None;
    }

    /// <summary>
    /// Determines whether the attachment parameters indicate a remote storage attachment.
    /// </summary>
    /// <param name="parameters">The retirement parameters to check. Can be null.</param>
    /// <returns>
    /// <c>true</c> if the parameters are not null and have flags set to Retired;
    /// otherwise, <c>false</c>.
    /// </returns>
    public static bool IsRetiredStorageAttachment(this RetireAttachmentParameters parameters)
    {
        if (parameters == null)
        {
            return false;
        }

        return parameters.Flags == RetiredAttachmentFlags.Retired;
    }

    private static RetireAttachmentParameters GetRetireAttachmentParameters(string identifier, DateTime? retireAt, RetiredAttachmentFlags flags)
    {
        RetireAttachmentParameters retireParameters = null;
        if (retireAt.HasValue)
        {
            retireParameters = new RetireAttachmentParameters(identifier, retireAt.Value) { Flags = flags };
        }

        return retireParameters;
    }

    internal static RetireAttachmentParameters GetRetireAttachmentParameters(LazyStringValue identifier, DateTime? retireAt, RetiredAttachmentFlags flags)
    {
        return GetRetireAttachmentParameters(identifier.ToString(), retireAt, flags);
    }
}
