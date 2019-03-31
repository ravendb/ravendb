namespace Raven.Server.Documents.TimeSeries
{
    public struct TimestampEncodingDetails
    {
        public int BitsForValue;
        public int ControlValue;
        public int ControlValueBitLength;
        public long MaxValueForEncoding;

        public TimestampEncodingDetails(int bitsForValue, int controlValue, int controlValueBitLength)
        {
            BitsForValue = bitsForValue;
            ControlValue = controlValue;
            ControlValueBitLength = controlValueBitLength;
            MaxValueForEncoding = 1L << (BitsForValue - 1);
        }

        public static readonly TimestampEncodingDetails[] Encodings = {
            new TimestampEncodingDetails(bitsForValue : 7,  controlValue : 0b10,   controlValueBitLength : 2),
            new TimestampEncodingDetails(bitsForValue : 9,  controlValue : 0b110,  controlValueBitLength : 3),
            new TimestampEncodingDetails(bitsForValue : 12, controlValue : 0b1110, controlValueBitLength : 4),
            new TimestampEncodingDetails(bitsForValue : 32, controlValue : 0b1111, controlValueBitLength : 4)
        };

        public static int MaxControlBitLength => Encodings[Encodings.Length - 1].ControlValueBitLength;
    }
}
