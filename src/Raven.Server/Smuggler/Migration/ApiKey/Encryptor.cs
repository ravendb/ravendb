using System;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    public static class Encryptor
    {
        static Encryptor()
        {
            Current = new DefaultEncryptor();
        }

        public static IEncryptor Current { get; private set; }

        public static Lazy<bool> IsFipsEnabled
        {
            get
            {
                return new Lazy<bool>(() =>
                {
                    try
                    {
                        var defaultEncryptor = new DefaultEncryptor();
                        defaultEncryptor.Hash.Compute16(new byte[] { 1 });

                        return false;
                    }
                    catch (Exception)
                    {
                        return true;
                    }
                });
            }
        }

        public static void Initialize(bool useFips)
        {
            Current = useFips ? (IEncryptor)new FipsEncryptor() : new DefaultEncryptor();
        }
    }
}