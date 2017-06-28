using System;
using System.Globalization;
using System.IO;
using Raven.Client.Exceptions.Security;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.Authentication
{
    public static class SignedTokenGenerator
    {
        private static readonly string EmptySignature = new string(' ',
            Sparrow.Utils.Base64.CalculateAndValidateOutputLength(Sodium.crypto_sign_bytes()));

        [ThreadStatic] private static byte[] _signatureBuffer;

        public static unsafe ArraySegment<byte> GenerateToken(
            JsonOperationContext context,
            byte[] signSecretKey,
            string apiKeyName,
            string nodeTag,
            DateTime expires)
        {
            if (_signatureBuffer == null)
                _signatureBuffer = new byte[Sodium.crypto_sign_bytes()];

            var ms = new MemoryStream();
            using (var writer = new BlittableJsonTextWriter(context, ms))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Sig");
                writer.WriteString((string)EmptySignature); // placeholder for signature
                writer.WriteComma();
                writer.WritePropertyName("Name");
                writer.WriteString(apiKeyName);
                writer.WriteComma();
                writer.WritePropertyName("Node");
                writer.WriteString(nodeTag);
                writer.WriteComma();
                writer.WritePropertyName("Expires");
                writer.WriteString(expires.ToString("O", CultureInfo.InvariantCulture));
                writer.WriteEndObject();
                writer.Flush();
            }

            ms.TryGetBuffer(out var buffer);

            fixed (byte* msg = buffer.Array)
            fixed (byte* sig = _signatureBuffer)
            fixed (byte* sk = signSecretKey)
            {
                if (Sodium.crypto_sign_detached(sig, null, msg + buffer.Offset, (ulong)buffer.Count, sk) != 0)
                    throw new AuthenticationException($"Unable to create and sign Access Token in node {nodeTag} for api key: {apiKeyName} ");

                // Copies the signature into the placeholder
                Sparrow.Utils.Base64.ConvertToBase64Array(msg + 8, sig, 0, _signatureBuffer.Length);

                ms.TryGetBuffer(out buffer);
                return buffer;
            }
        }
    }
}