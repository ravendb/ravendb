namespace Corax;

public enum IndexEntryTableEncoding : byte
{
    None = 0b00, // shouldn't be used ever
    OneByte = 0b01,
    TwoBytes = 0b10,
    FourBytes = 0b11, // We HAVE to ensure that this fits in two bits
}
