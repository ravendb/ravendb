using System;
using System.Runtime.CompilerServices;

namespace Voron.Data
{
    /// <summary>
    /// When a tree removed an entry from a branch so it has a single child, it collapse the remaining child page to itself.
    /// That subtree gets shallower, and on the _next_ split, we should split downward to keep the average height of the tree consistent.
    /// See RavenDB-27533.
    /// </summary>
    public static class PageCollapsedLevels
    {
        /// <summary>
        /// A page only owes another level if it collapses again before it splits, so even one is rare, three is huge.
        /// Kept small because we store the count in spare bits of the page flags.
        /// </summary>
        public const int Max = Mask >> Shift;

        // page types already wuse the bottom three bits
        private const int Shift = 3;
        public const byte Mask = 0b_11_000;

        public static int Get<TFlags>(TFlags pageFlags) where TFlags : unmanaged, Enum =>
            (ToByte(pageFlags) & Mask) >> Shift;

        public static TFlags Set<TFlags>(TFlags pageFlags, int value) where TFlags : unmanaged, Enum =>
            FromByte<TFlags>((byte)((ToByte(pageFlags) & ~Mask) | (Math.Clamp(value, 0, Max) << Shift)));

        public static TFlags PageType<TFlags>(TFlags pageFlags) where TFlags : unmanaged, Enum =>
            FromByte<TFlags>((byte)(ToByte(pageFlags) & ~Mask));

        public static TFlags SetPageType<TFlags>(TFlags pageFlags, TFlags pageType) where TFlags : unmanaged, Enum =>
            FromByte<TFlags>((byte)((ToByte(pageFlags) & Mask) | ToByte(pageType)));

        private static byte ToByte<TFlags>(TFlags pageFlags) where TFlags : unmanaged, Enum => Unsafe.BitCast<TFlags, byte>(pageFlags);

        private static TFlags FromByte<TFlags>(byte pageFlags) where TFlags : unmanaged, Enum => Unsafe.BitCast<byte, TFlags>(pageFlags);
    }
}
