using System;
using System.IO;
using System.Security.Cryptography;

namespace Sparrow.Server.Utils
{
    public static class FileHelper
    {
        public static string CalculateHash(string filePath)
        {
            if (File.Exists(filePath) == false)
                return null;

            using var stream = File.OpenRead(filePath);
            using var hash = SHA256.Create();
            return Convert.ToBase64String(hash.ComputeHash(stream));
        }
    }
}
