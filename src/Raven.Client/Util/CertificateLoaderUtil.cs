using System;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using Sparrow.Logging;

namespace Raven.Client;

internal static class CertificateLoaderUtil
{
    private static readonly Logger Log = LoggingSource.Instance.GetLogger("Server", nameof(CertificateLoaderUtil));
    
    private static bool FirstTime = true;
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
        Exception exception = null;
        try
        {
            importer(f);
        }
        catch (Exception e)
        {
            exception = e;
            f = SwitchUserKeySetWithMachineKeySet(f);
            importer(f);
        }

        LogIfNeeded(nameof(Import), f, exception);
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
        Exception exception = null;
        X509Certificate2 ret;
        try
        {
            ret = creator(f);
        }
        catch(Exception e)
        {
            exception = e;
            f = SwitchUserKeySetWithMachineKeySet(f);
            ret = creator(f);
        }
        
        LogIfNeeded(nameof(CreateCertificate), f, exception);
        return ret;
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
#if NETCOREAPP3_1_OR_GREATER 
            X509KeyStorageFlags.EphemeralKeySet |
#endif
            X509KeyStorageFlags.UserKeySet |
            X509KeyStorageFlags.MachineKeySet;
        
        Debug.Assert(flags.HasValue == false || (flags.Value & keyStorageFlags) == 0);
    }
    
    private static void LogIfNeeded(string method, X509KeyStorageFlags f, Exception exception)
    {
        if (FirstTime)
        {
            FirstTime = false;
            if (Log.IsOperationsEnabled)
                Log.Operations(CreateMsg());
        }
        else
        {
            if (Log.IsInfoEnabled)
                Log.Info(CreateMsg());
        }

        string CreateMsg()
        {
            var msg = $"{nameof(CertificateLoaderUtil)}.{method} - Flags used {f}";
            if (exception != null)
                msg += $"First attempt exception : {exception}";
            return msg;
        }
    }

}
