using System;
using System.Text.Encodings.Web;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SupplementaryPlaneJavaScriptEncoder : JavaScriptEncoder
    {
        public static readonly SupplementaryPlaneJavaScriptEncoder Instance = new();

        private static readonly JavaScriptEncoder Inner = UnsafeRelaxedJsonEscaping;

        private SupplementaryPlaneJavaScriptEncoder()
        {
        }

        public override int MaxOutputCharactersPerInputCharacter => Inner.MaxOutputCharactersPerInputCharacter;

        public override bool WillEncode(int unicodeScalar)
        {
            return unicodeScalar <= char.MaxValue && Inner.WillEncode(unicodeScalar);
        }

        public override unsafe bool TryEncodeUnicodeScalar(int unicodeScalar, char* buffer, int bufferLength, out int numberOfCharactersWritten)
        {
            return Inner.TryEncodeUnicodeScalar(unicodeScalar, buffer, bufferLength, out numberOfCharactersWritten);
        }

        public override unsafe int FindFirstCharacterToEncode(char* text, int textLength)
        {
            int offset = 0;
            while (offset < textLength)
            {
                int index = Inner.FindFirstCharacterToEncode(text + offset, textLength - offset);
                if (index < 0)
                    return -1;

                int position = offset + index;
                if (char.IsHighSurrogate(text[position]) == false || position + 1 >= textLength || char.IsLowSurrogate(text[position + 1]) == false)
                    return position;

                offset = position + 2;
            }

            return -1;
        }

        public override int FindFirstCharacterToEncodeUtf8(ReadOnlySpan<byte> utf8Text)
        {
            int offset = 0;
            while (offset < utf8Text.Length)
            {
                int index = Inner.FindFirstCharacterToEncodeUtf8(utf8Text.Slice(offset));
                if (index < 0)
                    return -1;

                int position = offset + index;
                if (IsSupplementaryScalar(utf8Text.Slice(position)) == false)
                    return position;

                offset = position + 4;
            }

            return -1;
        }

        private static bool IsSupplementaryScalar(ReadOnlySpan<byte> utf8)
        {
            if (utf8.Length < 4)
                return false;

            byte lead = utf8[0];
            byte second = utf8[1];
            if (lead < 0xF0 || lead > 0xF4 || (second & 0xC0) != 0x80)
                return false;

            if ((lead == 0xF0 && second < 0x90) || (lead == 0xF4 && second > 0x8F))
                return false;

            return (utf8[2] & 0xC0) == 0x80 && (utf8[3] & 0xC0) == 0x80;
        }
    }
}
