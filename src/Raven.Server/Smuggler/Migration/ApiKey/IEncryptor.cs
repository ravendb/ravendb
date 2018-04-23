namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public interface IEncryptor
    {
        IHashEncryptor Hash { get; }

        IHashEncryptor CreateHash();

        ISymmetricalEncryptor CreateSymmetrical();

        ISymmetricalEncryptor CreateSymmetrical(int keySize);

        IAsymmetricalEncryptor CreateAsymmetrical();

        IAsymmetricalEncryptor CreateAsymmetrical(byte[] exponent, byte[] modulus);

        IAsymmetricalEncryptor CreateAsymmetrical(int keySize);
    }
}