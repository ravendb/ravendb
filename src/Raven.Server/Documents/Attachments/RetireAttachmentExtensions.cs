using System;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

namespace Raven.Server.Documents.Attachments;

public static class RetireAttachmentExtensions
{
    /// <summary>
    /// Checks if the RetireParameters is null or its Flags property matches the specified flags.
    /// </summary>
    /// <param name="parameters">The attachment to check.</param>
    /// <param name="flags">The flags to compare with.</param>
    /// <returns>True if RetireParameters is null or its Flags property equals the specified flags; otherwise, false.</returns>
    public static bool IsLocalAttachment(this RetireAttachmentParameters parameters)
    {
        return parameters == null || parameters.Flags == RetiredAttachmentFlags.None;
    }

    public static bool IsRetiredAttachment(this RetireAttachmentParameters parameters)
    {
        if (parameters == null)
        {
            return false;
        }

        return parameters.Flags != RetiredAttachmentFlags.None;
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

    public static RetireAttachmentParameters GetRetireAttachmentParameters(LazyStringValue identifier, DateTime? retireAt, RetiredAttachmentFlags flags)
    {
        return GetRetireAttachmentParameters(identifier.ToString(), retireAt, flags);
    }
}
