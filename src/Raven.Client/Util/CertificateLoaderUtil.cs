using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Raven.Client;

internal static class CertificateLoaderUtil
{
    private static  int KeySetFlag = (int)X509KeyStorageFlags.UserKeySet;
        
    public static X509KeyStorageFlags FlagsForOpen => (X509KeyStorageFlags)KeySetFlag;

    public static X509KeyStorageFlags FlagsForExport => FlagsForOpen | X509KeyStorageFlags.Exportable;

    public static X509KeyStorageFlags FlagsForPersist => FlagsForOpen | X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;

    public static X509Certificate2 CreateCertificate(byte[] rawData, string password = null, X509KeyStorageFlags? flags = null)
    {
        return CreateCertificate(f => new X509Certificate2(rawData, password, f), flags);
    }
    internal static X509Certificate2 CreateCertificate(string fileName, string password = null, X509KeyStorageFlags? flags = null)
    {
        return CreateCertificate(f => new X509Certificate2(fileName, password, f), flags);
    }

    private static X509Certificate2 CreateCertificate(Func<X509KeyStorageFlags, X509Certificate2> creator, X509KeyStorageFlags? flag)
    {
        var f = flag ?? FlagsForOpen;
        try
        {
            return creator(f);
        }
        catch
        {
            if ((f & X509KeyStorageFlags.MachineKeySet) == X509KeyStorageFlags.MachineKeySet)
                throw;
            Volatile.Write(ref KeySetFlag, (int)X509KeyStorageFlags.MachineKeySet);
            return creator(f & ~X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.MachineKeySet);
        }
    }
}
