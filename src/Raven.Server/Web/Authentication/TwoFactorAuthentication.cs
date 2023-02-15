using System;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using JetBrains.Annotations;

namespace Raven.Server.Web.Authentication;

public static class TwoFactorAuthentication
{
    public static string CreateValidationCode(string twoFactorAuthenticationKey)
    {
        byte[] bytes = FromBase32(twoFactorAuthenticationKey);
        return Rfc6238AuthenticationService.Generate(bytes);
    }
    
    public static bool ValidateCode(string twoFactorAuthenticationKey, int token)
    {
        byte[] bytes = FromBase32(twoFactorAuthenticationKey);
        return Rfc6238AuthenticationService.ValidateCode(bytes, token);
    }
    
    public static string GenerateQrCodeUri(string secret, string host, string name)
    {
        string environmentName = host;
        int dotIdx = environmentName.IndexOf('.');
        if (dotIdx != -1)
        {
            var withoutNodePrefix = environmentName[(dotIdx + 1)..];
            if (string.IsNullOrEmpty(withoutNodePrefix) == false) // strip the a., etc
                environmentName = withoutNodePrefix;
        }

        string encodedIssuer = UrlEncoder.Default.Encode(environmentName);
        string encodedName = UrlEncoder.Default.Encode(name);
        return $"otpauth://totp/{encodedIssuer}:{encodedName}?secret={secret}&issuer={encodedIssuer}";
    }
    
    public static string GenerateSecret() => GenerateBase32();
    // Rfc6238Authentication taken from:
    // https://github.com/dotnet/aspnetcore/blob/6a7bcda42de7b98196b38924cc354216eba57c9b/src/Identity/Extensions.Core/src/Rfc6238AuthenticationService.cs#L15
    public static class Rfc6238AuthenticationService
    {
        private static readonly TimeSpan _timestep = TimeSpan.FromMinutes(3);
        private static readonly Encoding _encoding = new UTF8Encoding(false, true);

        
        internal static int ComputeTotp(byte[] key, ulong timestepNumber)
        {
            // # of 0's = length of pin
            const int Mod = 1000000;

            // See https://tools.ietf.org/html/rfc4226
            Span<byte> timestepAsBytes = stackalloc byte[sizeof(long)];
            var res = BitConverter.TryWriteBytes(timestepAsBytes, IPAddress.HostToNetworkOrder((long)timestepNumber));
            Debug.Assert(res);

            Span<byte> modifierCombinedBytes = timestepAsBytes;
  
            Span<byte> hash = stackalloc byte[HMACSHA1.HashSizeInBytes];
            res = HMACSHA1.TryHashData(key, modifierCombinedBytes, hash, out var written);
            Debug.Assert(res);
            Debug.Assert(written == hash.Length);

            // Generate DT string
            var offset = hash[^1] & 0xf;
            Debug.Assert(offset + 4 < hash.Length);
            var binaryCode = (hash[offset] & 0x7f) << 24
                             | (hash[offset + 1] & 0xff) << 16
                             | (hash[offset + 2] & 0xff) << 8
                             | (hash[offset + 3] & 0xff);

            return binaryCode % Mod;
        }
        
        // More info: https://tools.ietf.org/html/rfc6238#section-4
        private static ulong GetCurrentTimeStepNumber()
        {
            var delta = DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch;
            return (ulong)(delta.Ticks / _timestep.Ticks);
        }
        
        public static bool ValidateCode([NotNull] byte[] securityToken, int code)
        {
            if (securityToken == null) throw new ArgumentNullException(nameof(securityToken));

            // Allow a variance of no greater than 9 minutes in either direction
            var currentTimeStep = GetCurrentTimeStepNumber();

            for (var i = -2; i <= 2; i++)
            {
                var computedTotp = ComputeTotp(securityToken, (ulong)((long)currentTimeStep + i));
                if (computedTotp == code)
                    return true;
            }
            return false;
        }
        
                
        public static string Generate([NotNull] byte[] securityToken)
        {
            if (securityToken == null) throw new ArgumentNullException(nameof(securityToken));

            var currentTimeStep = GetCurrentTimeStepNumber();
            return ComputeTotp(securityToken, (ulong)((long)currentTimeStep)).ToString("D6", CultureInfo.InvariantCulture);
        }
    }
    
    
    // base 32 taken from:
    // https://github.com/dotnet/aspnetcore/blob/74389644ecb9f7abc63b2d54bf615e1c3887ffd5/src/Identity/Extensions.Core/src/Base32.cs
    private const string _base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    
    private static string GenerateBase32()
    {
        const int length = 20;
        // base32 takes 5 bytes and converts them into 8 characters, which would be (byte length / 5) * 8
        // except that it also pads ('=') for the last processed chunk if it's less than 5 bytes.
        // So in order to handle the padding we add 1 less than the chunk size to our byte length
        // which will either be removed due to integer division truncation if the length was already a multiple of 5
        // or it will increase the divided length by 1 meaning that a 1-4 byte length chunk will be 1 instead of 0
        // so the padding is now included in our string length calculation
        return string.Create(((length + 4) / 5) * 8, 0, static (buffer, _) =>
        {
            Span<byte> bytes = stackalloc byte[length];
            RandomNumberGenerator.Fill(bytes);

            var index = 0;
            for (int offset = 0; offset < bytes.Length;)
            {
                byte a, b, c, d, e, f, g, h;
                int numCharsToOutput = GetNextGroup(bytes, ref offset, out a, out b, out c, out d, out e, out f, out g, out h);

                buffer[index + 7] = ((numCharsToOutput >= 8) ? _base32Chars[h] : '=');
                buffer[index + 6] = ((numCharsToOutput >= 7) ? _base32Chars[g] : '=');
                buffer[index + 5] = ((numCharsToOutput >= 6) ? _base32Chars[f] : '=');
                buffer[index + 4] = ((numCharsToOutput >= 5) ? _base32Chars[e] : '=');
                buffer[index + 3] = ((numCharsToOutput >= 4) ? _base32Chars[d] : '=');
                buffer[index + 2] = (numCharsToOutput >= 3) ? _base32Chars[c] : '=';
                buffer[index + 1] = (numCharsToOutput >= 2) ? _base32Chars[b] : '=';
                buffer[index] = (numCharsToOutput >= 1) ? _base32Chars[a] : '=';
                index += 8;
            }
        });
    }

    // returns the number of bytes that were output
    private static int GetNextGroup(Span<byte> input, ref int offset, out byte a, out byte b, out byte c, out byte d, out byte e, out byte f, out byte g, out byte h)
    {
        uint b1, b2, b3, b4, b5;

        int retVal;
        switch (input.Length - offset)
        {
            case 1: retVal = 2; break;
            case 2: retVal = 4; break;
            case 3: retVal = 5; break;
            case 4: retVal = 7; break;
            default: retVal = 8; break;
        }

        b1 = (offset < input.Length) ? input[offset++] : 0U;
        b2 = (offset < input.Length) ? input[offset++] : 0U;
        b3 = (offset < input.Length) ? input[offset++] : 0U;
        b4 = (offset < input.Length) ? input[offset++] : 0U;
        b5 = (offset < input.Length) ? input[offset++] : 0U;

        a = (byte)(b1 >> 3);
        b = (byte)(((b1 & 0x07) << 2) | (b2 >> 6));
        c = (byte)((b2 >> 1) & 0x1f);
        d = (byte)(((b2 & 0x01) << 4) | (b3 >> 4));
        e = (byte)(((b3 & 0x0f) << 1) | (b4 >> 7));
        f = (byte)((b4 >> 2) & 0x1f);
        g = (byte)(((b4 & 0x3) << 3) | (b5 >> 5));
        h = (byte)(b5 & 0x1f);

        return retVal;
    }
    
    public static byte[] FromBase32([NotNull] string input)
    {
        if (input == null) throw new ArgumentNullException(nameof(input));
        var trimmedInput = input.AsSpan().TrimEnd('=');
        if (trimmedInput.Length == 0)
        {
            return Array.Empty<byte>();
        }

        var output = new byte[trimmedInput.Length * 5 / 8];
        var bitIndex = 0;
        var inputIndex = 0;
        var outputBits = 0;
        var outputIndex = 0;
        while (outputIndex < output.Length)
        {
            var byteIndex = _base32Chars.IndexOf(char.ToUpperInvariant(trimmedInput[inputIndex]));
            if (byteIndex < 0)
            {
                throw new FormatException();
            }

            var bits = Math.Min(5 - bitIndex, 8 - outputBits);
            output[outputIndex] <<= bits;
            output[outputIndex] |= (byte)(byteIndex >> (5 - (bitIndex + bits)));

            bitIndex += bits;
            if (bitIndex >= 5)
            {
                inputIndex++;
                bitIndex = 0;
            }

            outputBits += bits;
            if (outputBits >= 8)
            {
                outputIndex++;
                outputBits = 0;
            }
        }
        return output;
    }
}
