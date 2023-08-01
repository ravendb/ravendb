namespace Raven.Server.Commercial
{
    public sealed class RSAPublicParameters
    {
        public RSAKeyValue RsaKeyValue { get; set; }

        public sealed class RSAKeyValue
        {
            public byte[] Modulus { get; set; }

            public byte[] Exponent { get; set; }
        }
    }
}