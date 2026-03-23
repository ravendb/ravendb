namespace Sparrow.Json;
#if DEBUG
public sealed unsafe partial class BlittableJsonReaderObject
{
    public string DebugMemory => DebugMemoryHelper.GetDebugView(_mem, _size);
}
#endif
