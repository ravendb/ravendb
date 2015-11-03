using System;
using System.Text;

using Raven.Abstractions.Util.Encryptors;

namespace Raven.Client.Connection
{
    public static class ServerHash
    {
        public static string GetServerHash(string url)
        {
            var bytes = Encoding.UTF8.GetBytes(url);
            return BitConverter.ToString(GetHash(bytes));
        }

        private static byte[] GetHash(byte[] bytes)
        {
            return Encryptor.Current.Hash.Compute16(bytes);
        }
    }
}
