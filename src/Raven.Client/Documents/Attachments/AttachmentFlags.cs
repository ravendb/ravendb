using System;
using System.Runtime.CompilerServices;

namespace Raven.Client.Documents.Attachments
{
    [Flags]
    public enum AttachmentFlags
    {
        None = 0,
        Retired = 0x1,
        Compressed = 0x2
    }

    public static class EnumExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this AttachmentFlags current, AttachmentFlags flag)
        {
            return (current & flag) == flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static AttachmentFlags Strip(this AttachmentFlags current, AttachmentFlags flag)
        {
            return current & ~flag;
        }
    }
}
