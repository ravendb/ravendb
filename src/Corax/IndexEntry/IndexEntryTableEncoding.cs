namespace Corax;


// PERF: This entries are used in a lookup table at the IndexEntryReader. Any modification of these values
//       would require the modification of the table. 
public enum IndexEntryTableEncoding : byte
{
    None = 0b00, // shouldn't be used ever
    OneByte = 0b01,
    TwoBytes = 0b10,
    FourBytes = 0b11, // We HAVE to ensure that this fits in two bits
}
