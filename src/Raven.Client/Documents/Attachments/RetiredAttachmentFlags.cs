using System;
using System.Runtime.CompilerServices;

namespace Raven.Client.Documents.Attachments
{
    [Flags]
    public enum RetiredAttachmentFlags
    {
        None = 0,
        Retired = 0x1
    }

    internal static class EnumExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this RetiredAttachmentFlags current, RetiredAttachmentFlags flag)
        {
            return (current & flag) == flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RetiredAttachmentFlags Strip(this RetiredAttachmentFlags current, RetiredAttachmentFlags flag)
        {
            return current & ~flag;
        }
    }
}
