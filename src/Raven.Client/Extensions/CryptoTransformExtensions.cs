using System.IO;
using System.Security.Cryptography;

namespace Raven.NewClient.Abstractions.Extensions
{
    public static class CryptoTransformExtensions
    {
        public static byte[] TransformEntireBlock(this ICryptoTransform transform, byte[] data)
        {
            using (var input = new MemoryStream(data))
            using (var output = new CryptoStream(input, transform, CryptoStreamMode.Read))
            using (var result = new MemoryStream())
            {
                output.CopyTo(result);
                return result.ToArray();
            }
        }
    }
}
