using Sparrow.Json;

namespace Voron;
#if DEBUG
public unsafe partial struct Slice
{
    public string DebugMemory => DebugMemoryHelper.GetDebugView(Content.Ptr, Content.Length);
}
#endif
