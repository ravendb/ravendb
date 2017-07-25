using System;

namespace Sparrow.Utils
{
    public class Base64
    {
        // This code was taken from: https://github.com/dotnet/coreclr/blob/master/src/mscorlib/shared/System/Convert.cs


        internal static readonly char[] base64Table =
        {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
            'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
            'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
            't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', '+', '/', '='
        };


        public static int CalculateAndValidateOutputLength(int inputLength)
        {
            long outlen = ((long)inputLength) / 3 * 4;          // the base length - we want integer division here. 
            outlen += ((inputLength % 3) != 0) ? 4 : 0;         // at most 4 more chars for the remainder

            if (outlen == 0)
                return 0;

            // If we overflow an int then we cannot allocate enough
            // memory to output the value so throw
            if (outlen > int.MaxValue)
                throw new OutOfMemoryException();

            return (int)outlen;
        }

        public static unsafe int ConvertToBase64Array(char* outChars, byte* inData, int offset, int length)
        {
            int lengthmod3 = length % 3;
            int calcLength = offset + (length - lengthmod3);
            int j = 0;
            //Convert three bytes at a time to base64 notation.  This will consume 4 chars.
            int i;

            // get a pointer to the base64Table to avoid unnecessary range checking
            fixed (char* base64 = &base64Table[0])
            {
                for (i = offset; i < calcLength; i += 3)
                {
                    outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                    outChars[j + 1] = base64[((inData[i] & 0x03) << 4) | ((inData[i + 1] & 0xf0) >> 4)];
                    outChars[j + 2] = base64[((inData[i + 1] & 0x0f) << 2) | ((inData[i + 2] & 0xc0) >> 6)];
                    outChars[j + 3] = base64[(inData[i + 2] & 0x3f)];
                    j += 4;
                }

                //Where we left off before
                i = calcLength;

                switch (lengthmod3)
                {
                    case 2: //One character padding needed
                        outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                        outChars[j + 1] = base64[((inData[i] & 0x03) << 4) | ((inData[i + 1] & 0xf0) >> 4)];
                        outChars[j + 2] = base64[(inData[i + 1] & 0x0f) << 2];
                        outChars[j + 3] = base64[64]; //Pad
                        j += 4;
                        break;
                    case 1: // Two character padding needed
                        outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                        outChars[j + 1] = base64[(inData[i] & 0x03) << 4];
                        outChars[j + 2] = base64[64]; //Pad
                        outChars[j + 3] = base64[64]; //Pad
                        j += 4;
                        break;
                }
            }

            return j;
        }

        public static unsafe int ConvertToBase64ArrayUnpadded(char* outChars, byte* inData, int offset, int length)
        {
            int lengthmod3 = length % 3;
            int calcLength = offset + (length - lengthmod3);
            int j = 0;
            //Convert three bytes at a time to base64 notation.  This will consume 4 chars.
            int i;

            // get a pointer to the base64Table to avoid unnecessary range checking
            fixed (char* base64 = &base64Table[0])
            {
                for (i = offset; i < calcLength; i += 3)
                {
                    outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                    outChars[j + 1] = base64[((inData[i] & 0x03) << 4) | ((inData[i + 1] & 0xf0) >> 4)];
                    outChars[j + 2] = base64[((inData[i + 1] & 0x0f) << 2) | ((inData[i + 2] & 0xc0) >> 6)];
                    outChars[j + 3] = base64[(inData[i + 2] & 0x3f)];
                    j += 4;
                }

                //Where we left off before
                i = calcLength;

                switch (lengthmod3)
                {
                    case 2: //One character padding needed
                        outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                        outChars[j + 1] = base64[((inData[i] & 0x03) << 4) | ((inData[i + 1] & 0xf0) >> 4)];
                        outChars[j + 2] = base64[(inData[i + 1] & 0x0f) << 2];
                        j += 3;
                        break;
                    case 1: // Two character padding needed
                        outChars[j] = base64[(inData[i] & 0xfc) >> 2];
                        outChars[j + 1] = base64[(inData[i] & 0x03) << 4];
                        j += 2;
                        break;
                }
            }

            return j;
        }
    }
}