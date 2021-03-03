namespace Sparrow
{
    public readonly unsafe struct UnmanagedPointer
    {
        public readonly byte* Address;

        public UnmanagedPointer(byte* address)
        {
            Address = address;
        }

        public static UnmanagedPointer operator +(UnmanagedPointer pointer, int offset)
            => new UnmanagedPointer(pointer.Address + offset);
    }
}
