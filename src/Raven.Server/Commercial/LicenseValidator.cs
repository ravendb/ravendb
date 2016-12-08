using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;

namespace Raven.Server.Commercial
{
    public static class LicenseValidator
    {
        private const int TypeBitsToShift = 5;

        private static readonly string[] Terms =
        {
            "id","expiration","type","version","maxRamUtilization","maxParallelism",
            "allowWindowsClustering","OEM","numberOfDatabases","fips","periodicBackup",
            "quotas","authorization","documentExpiration","replication","versioning",
            "maxSizeInGb","ravenfs","encryption","compression","updatesExpiration",
        };

        private enum ValueType : byte
        {
            False = 0,
            True = 1,
            Int = 2,
            Date = 3,
        }

        private static DateTime FromDosDate(ushort number)
        {
            var year = (number >> 9) + 1980;
            var month = (number & 0x01e0) >> 5;
            var day = number & 0x1F;
            return new DateTime(year, month, day);
        }

        private static byte[] GetBytes(string str)
        {
            return Convert.FromBase64String(str);
        }

        public static Dictionary<string, object> Validate(License licenseKey, RSAParameters rsAParameters)
        {
            var result = new Dictionary<string, object>();
            using (var ms = new MemoryStream())
            using (var br = new BinaryReader(ms))
            {
                var buffer = GetBytes(licenseKey.Keys.First());
                ms.Write(buffer, 0, buffer.Length);
                ms.Position = 0;

                while (ms.Position < buffer.Length)
                {
                    var token = ms.ReadByte();
                    var index = token & 0x1F;
                    object val;
                    var curr = (ValueType)(token >> TypeBitsToShift);
                    switch (curr)
                    {
                        case ValueType.False:
                            val = false;
                            break;
                        case ValueType.True:
                            val = true;
                            break;
                        case ValueType.Int:
                            val = (int)br.ReadByte();
                            break;
                        case ValueType.Date:
                            val = FromDosDate(br.ReadUInt16());
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (index >= Terms.Length)
                        continue; // new field, just skip
                    result[Terms[index]] = val;
                }

                var attributesLen = ms.Position;
                ms.SetLength(attributesLen);

                new BinaryWriter(ms).Write(licenseKey.Id.ToByteArray());
                new BinaryWriter(ms).Write(licenseKey.Name);

                using (var rsa = new RSACryptoServiceProvider())
                {
                    rsa.ImportParameters(rsAParameters);

                    using (var sha1 = SHA1.Create())
                    {
                        ms.Position = 0;
                        var hash = sha1.ComputeHash(ms);

                        var signature = licenseKey.Keys.Last();
                        if (rsa.VerifyHash(hash, GetBytes(signature), HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1) == false)
                            throw new InvalidDataException("Could not validate signature on license");
                    }
                }

                return result;
            }
        }
    }
}