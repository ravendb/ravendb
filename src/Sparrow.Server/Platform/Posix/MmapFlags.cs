using System;

namespace Sparrow.Server.Platform.Posix
{
    [Flags]
    public enum MmapFlags : int
    {
        MAP_SHARED = 0x01,     // Share changes.
        MAP_PRIVATE = 0x02,     // Changes are private.
        MAP_TYPE = 0x0f,     // Mask for type of mapping.
        MAP_FIXED = 0x10,     // Interpret addr exactly.
        MAP_FILE = 0,
        MAP_ANONYMOUS = 0x20,     // Don't use a file.
        MAP_ANON = MAP_ANONYMOUS,

        // These are Linux-specific.
        MAP_GROWSDOWN = 0x00100,  // Stack-like segment.
        MAP_DENYWRITE = 0x00800,  // ETXTBSY
        MAP_EXECUTABLE = 0x01000,  // Mark it as an executable.
        MAP_LOCKED = 0x02000,  // Lock the mapping.
        MAP_NORESERVE = 0x04000,  // Don't check for reservations.
        MAP_POPULATE = 0x08000,  // Populate (prefault) pagetables.
        MAP_NONBLOCK = 0x10000,  // Do not block on IO.
    }
}
