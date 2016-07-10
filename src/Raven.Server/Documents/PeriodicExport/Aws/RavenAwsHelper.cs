// -----------------------------------------------------------------------
//  <copyright file="RavenAwsHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Server.Documents.PeriodicExport.Aws
{
    public static class RavenAwsHelper
    {
        private const int OneMegabyte = 1024 * 1024;

        public static string ConvertToString(DateTime date)
        {
            return date.ToString("yyyyMMddTHHmmssZ");
        }

        public static string ConvertToHex(byte[] array)
        {
            return BitConverter.ToString(array).Replace("-", "").ToLower();
        }

        public static string CalculatePayloadHash(Stream stream)
        {
            using (var hash = SHA256.Create())
            {
                var hashedPayload = ConvertToHex(stream != null ? hash.ComputeHash(stream) : hash.ComputeHash(Encoding.UTF8.GetBytes(string.Empty)));
                if (stream != null)
                    stream.Position = 0;

                return hashedPayload;
            }
        }

        /**
        * Computes the SHA-256 tree hash for the given file
        * 
        * @param inputFile
        *            A file to compute the SHA-256 tree hash for
        * @return a byte[] containing the SHA-256 tree hash
        */
        public static string CalculatePayloadTreeHash(Stream stream)
        {
            var chunkSha256Hashes = GetChunkSha256Hashes(stream);
            stream.Position = 0;

            return ConvertToHex(ComputeSha256TreeHash(chunkSha256Hashes));
        }

        /**
         * Computes a SHA256 checksum for each 1 MB chunk of the input file. This
         * includes the checksum for the last chunk even if it is smaller than 1 MB.
         * 
         * @param file
         *            A file to compute checksums on
         * @return a byte[][] containing the checksums of each 1MB chunk
         */
        private static byte[] CalculateSha256Hash(byte[] inputBytes, int count)
        {
            using (var hash = SHA256.Create()) 
                return hash.ComputeHash(inputBytes, 0, count);
        }

        private static byte[] ComputeSha256TreeHash(byte[][] chunkSha256Hashes)
        {
            byte[][] prevLvlHashes = chunkSha256Hashes;
            while (prevLvlHashes.GetLength(0) > 1)
            {
                int len = prevLvlHashes.GetLength(0) / 2;
                if (prevLvlHashes.GetLength(0) % 2 != 0)
                {
                    len++;
                }

                var currLvlHashes = new byte[len][];

                int j = 0;
                for (int i = 0; i < prevLvlHashes.GetLength(0); i = i + 2, j++)
                {
                    // If there are at least two elements remaining
                    if (prevLvlHashes.GetLength(0) - i > 1)
                    {
                        // Calculate a digest of the concatenated nodes
                        byte[] firstPart = prevLvlHashes[i];
                        byte[] secondPart = prevLvlHashes[i + 1];
                        var concatenation = new byte[firstPart.Length + secondPart.Length];
                        Buffer.BlockCopy(firstPart, 0, concatenation, 0, firstPart.Length);
                        Buffer.BlockCopy(secondPart, 0, concatenation, firstPart.Length, secondPart.Length);

                        currLvlHashes[j] = CalculateSha256Hash(concatenation, concatenation.Length);
                    }
                    else
                    {
                        // Take care of remaining odd chunk
                        currLvlHashes[j] = prevLvlHashes[i];
                    }
                }

                prevLvlHashes = currLvlHashes;
            }

            return prevLvlHashes[0];
        }

        private static byte[][] GetChunkSha256Hashes(Stream stream)
        {
            long numChunks = stream.Length / OneMegabyte;
            if (stream.Length % OneMegabyte > 0)
            {
                numChunks++;
            }

            if (numChunks == 0)
            {
                return new[] { CalculateSha256Hash(null, 0) };
            }

            var chunkSha256Hashes = new byte[(int)numChunks][];

            var buff = new byte[OneMegabyte];

            int bytesRead;
            int idx = 0;

            while ((bytesRead = stream.Read(buff, 0, OneMegabyte)) > 0)
            {
                chunkSha256Hashes[idx++] = CalculateSha256Hash(buff, bytesRead);
            }

            return chunkSha256Hashes;
        }
    }
}
