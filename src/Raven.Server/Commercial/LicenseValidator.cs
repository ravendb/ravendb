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
        private const int Mask = 0x1F;

        private static readonly LicenseAttribute[] Terms = (LicenseAttribute[])Enum.GetValues(typeof(LicenseAttribute));

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

        public static Dictionary<LicenseAttribute, object> Validate(License licenseKey, RSAParameters rsaParameters)
        {
            var licenseBinary = Convert.FromBase64String(string.Join(string.Empty, licenseKey.Keys));
            byte[] actualLicenseData = licenseBinary[128..];
            byte[] licenseSignature = licenseBinary[..128];

            using (var ms = new MemoryStream())
            using (var rsa = RSA.Create())
            using (var binaryWriter = new BinaryWriter(ms))
            {
                ms.Write(actualLicenseData);
                binaryWriter.Write(licenseKey.Id.ToByteArray());
                binaryWriter.Write(licenseKey.Name);

                var licenseToCheck = ms.ToArray();

                rsa.ImportParameters(rsaParameters);
                bool verifyHash;

                using (var sha256 = SHA256.Create()) // first try with SHA256 (newer licenses)
                {
                    var hash = sha256.ComputeHash(licenseToCheck);

                    verifyHash = rsa.VerifyHash(hash, licenseSignature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                }

                if (verifyHash == false) // if this failed, let's use using SHA1, instead
                {
                    using (var sha1 = SHA1.Create())
                    {
                        var hash = sha1.ComputeHash(licenseToCheck);

                        verifyHash = rsa.VerifyHash(hash, licenseSignature, HashAlgorithmName.SHA1, RSASignaturePadding.Pkcs1);
                    }
                }
            

                if (verifyHash == false)
                    throw new InvalidDataException("Could not validate signature on license");
            }

            var result = new Dictionary<LicenseAttribute, object>();
            using (var ms = new MemoryStream(actualLicenseData))
            using (var br = new BinaryReader(ms))
            {
                while (ms.Position < ms.Length)
                {
                    var licensePropertyExists = TryGetTermIndexAndType(br, out var licenseProperty, out ValueType type);

                    object val = type switch
                    {
                        ValueType.False => false,
                        ValueType.True => true,
                        ValueType.Int8 => (int)br.ReadByte(),
                        ValueType.Int32 => br.ReadInt32(),
                        ValueType.Date => FromDosDate(br.ReadUInt16()),
                        ValueType.String => Encoding.UTF8.GetString(br.ReadBytes((int)br.ReadByte())),
                        _ => throw new ArgumentOutOfRangeException(),
                    };

                    if (licensePropertyExists == false)
                    {
                        //new license property
                        continue;
                    }

                    result[licenseProperty] = val;
                }

                return result;
            }
        }

        private static bool TryGetTermIndexAndType(BinaryReader br, out LicenseAttribute licenseProperty, out ValueType type)
        {
            var token = br.ReadByte();
            var index = token & Mask;
            if (index == Mask)
            {
                //extended term
                br.ReadByte(); //discard legacy value
                ushort val = br.ReadUInt16();
                index = val & 0x1FFF;
                type = (ValueType)(val >> (8 + TypeBitsToShift));
            }
            else
            {
                type = (ValueType)(token >> TypeBitsToShift);
            }

            if (index >= Terms.Length)
            {
                licenseProperty = default;
                return false;
            }

            licenseProperty = Terms[index];
            return true;
        }
    }
}
