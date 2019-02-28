using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Server.Commercial
{
    public static class LicenseValidator
    {
        private const int TypeBitsToShift = 5;

        private static readonly string[] Terms =
        {
            "type", "version", "expiration", "memory", "cores", "redist", "encryption", "snmp",
            "distributedCluster", "maxClusterSize", "snapshotBackup", "cloudBackup", "dynamicNodesDistribution",
            "externalReplication", "delayedExternalReplication", "ravenEtl", "sqlEtl", "highlyAvailableTasks",
            "pullReplicationAsHub", "pullReplicationAsSink", "encryptedBackup"
        };

        private enum ValueType : byte
        {
            False = 0,
            True = 1,
            Int8 = 2,
            Date = 3,
            String = 4,
            Int32 = 5
        }

        private static DateTime FromDosDate(ushort number)
        {
            var year = (number >> 9) + 1980;
            var month = (number & 0x01e0) >> 5;
            var day = number & 0x1F;
            return new DateTime(year, month, day);
        }

        private static byte[] GetBytesFromBase64String(string str)
        {
            return Convert.FromBase64String(str);
        }

        public static Dictionary<string, object> Validate(License licenseKey, RSAParameters rsaParameters)
        {
            var keys = ExtractKeys(licenseKey.Keys);

            var result = new Dictionary<string, object>();
            using (var ms = new MemoryStream())
            using (var br = new BinaryReader(ms))
            {
                var buffer = keys.Attributes;
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
                        case ValueType.Int8:
                            val = (int)br.ReadByte();
                            break;
                        case ValueType.Int32:
                            val = br.ReadInt32();
                            break;
                        case ValueType.Date:
                            val = FromDosDate(br.ReadUInt16());
                            break;
                        case ValueType.String:
                            var valLength = (int)br.ReadByte();
                            val = Encoding.UTF8.GetString(br.ReadBytes(valLength));
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

                using (var binaryWriter = new BinaryWriter(ms))
                using (var rsa = RSA.Create())
                {
                    binaryWriter.Write(licenseKey.Id.ToByteArray());
                    binaryWriter.Write(licenseKey.Name);

                    rsa.ImportParameters(rsaParameters);

                    using (var sha1 = SHA1.Create())
                    {
                        ms.Position = 0;
                        var hash = sha1.ComputeHash(ms);

                        if (rsa.VerifyHash(hash, keys.Signature, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1) == false)
                            throw new InvalidDataException("Could not validate signature on license");
                    }
                }

                return result;
            }
        }

        private class Keys
        {
            public byte[] Attributes { get; set; }

            public byte[] Signature { get; set; }
        }

        private static Keys ExtractKeys(IEnumerable<string> keys)
        {
            var keysArray = keys.ToArray();
            var stringKey = string.Join(string.Empty, keysArray);
            var keysByteArray = GetBytesFromBase64String(stringKey);
            var attributes = keysByteArray.Skip(128).Take(keysByteArray.Length - 128).ToArray();
            Array.Resize(ref keysByteArray, 128);

            return new Keys
            {
                Signature = keysByteArray,
                Attributes = attributes
            };
        }
    }
}
