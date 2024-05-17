namespace Sparrow.Server.Binary
{
    public readonly struct Bit(byte value)
    {
        public readonly byte Value = (byte) (value & 1);

        public bool IsSet => Value == 1;
    }
}
