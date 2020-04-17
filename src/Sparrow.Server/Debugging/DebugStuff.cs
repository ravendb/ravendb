namespace Sparrow.Server.Debugging
{
    internal static class DebugStuff
    {
        public static void Attach()
        {
#if MEM_GUARD_STACK
            Sparrow.Debugging.DebugStuff.ElectricFencedMemory = ElectricFencedMemory.Instance;
#endif
        }
    }
}
