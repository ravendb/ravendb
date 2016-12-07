namespace Raven.Server.Commercial
{
    public class RSAPublicParameters
    {
        public RSAKeyValue RsaKeyValue { get; set; }

        public class RSAKeyValue
        {
            public byte[] Modulus { get; set; }

            public byte[] Exponent { get; set; }
        }
    }
}