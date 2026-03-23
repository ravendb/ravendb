namespace Sparrow.Json;
#if DEBUG
public unsafe partial class  LazyStringValue 
{
    public string DebugMemory => DebugMemoryHelper.GetDebugView(_buffer, _size);
}
#endif
