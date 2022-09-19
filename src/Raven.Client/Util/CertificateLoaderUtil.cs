using System;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Raven.Client;

internal static class CertificateLoaderUtil
{
    public static X509KeyStorageFlags FlagsForExport => X509KeyStorageFlags.Exportable;

    public static X509KeyStorageFlags FlagsForPersist => X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;

    public static void Import(X509Certificate2Collection collection, byte[] rawData, string password = null, X509KeyStorageFlags? flags = null)
    {
        Import(f => collection.Import(rawData, password, f), flags);
    }
    internal static void Import(X509Certificate2Collection collection, string fileName, string password = null, X509KeyStorageFlags? flags = null)
    {
        Import(f => collection.Import(fileName, password, f), flags);
    }
    
    private static void Import(Action<X509KeyStorageFlags> importer, X509KeyStorageFlags? flag)
    {
        DebugAssertDoesntContainKeySet(flag);
        var f = AddUserKeySet(flag);
        try
        {
            importer(f);
        }
        catch
        {
            importer(SwitchUserKeySetWithMachineKeySet(f));
        }
    }
    
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
        DebugAssertDoesntContainKeySet(flag);
        var f = AddUserKeySet(flag);
        try
        {
            return creator(f);
        }
        catch
        {
            return creator(SwitchUserKeySetWithMachineKeySet(f));
        }
    }

    private static X509KeyStorageFlags SwitchUserKeySetWithMachineKeySet(X509KeyStorageFlags f)
    {
        return f & ~X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.MachineKeySet;
    }

    private static X509KeyStorageFlags AddUserKeySet(X509KeyStorageFlags? flag)
    {
        return (flag ?? X509KeyStorageFlags.DefaultKeySet) | X509KeyStorageFlags.UserKeySet;
    }

    [Conditional("DEBUG")]
    private static void DebugAssertDoesntContainKeySet(X509KeyStorageFlags? flags)
    {
        const X509KeyStorageFlags keyStorageFlags = 
            X509KeyStorageFlags.UserKeySet | 
            X509KeyStorageFlags.MachineKeySet | 
            (X509KeyStorageFlags)0x20 /*X509KeyStorageFlags.EphemeralKeySet*/;
        
        Debug.Assert(flags.HasValue == false || (flags.Value & keyStorageFlags) == 0);
    }
}
