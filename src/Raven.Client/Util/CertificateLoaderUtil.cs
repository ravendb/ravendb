using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Logging;
using Sparrow.Logging;

namespace Raven.Client.Util;

internal static class CertificateLoaderUtil
{
    private static readonly IRavenLogger Logger = RavenLogManager.Instance.GetLoggerForClient(typeof(CertificateLoaderUtil));

    private static bool FirstTime = true;
    public static X509KeyStorageFlags FlagsForExport => X509KeyStorageFlags.Exportable;

    public static X509KeyStorageFlags FlagsForPersist => X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;

    public static void Import(X509Certificate2Collection collection, byte[] rawData, string password = null, X509KeyStorageFlags? flags = null)
    {
        DebugAssertDoesntContainKeySet(flags);
        var f = AddUserKeySet(flags);

        Exception exception = null;
        try
        {
            ImportCertificate(collection, rawData, password, f);
        }
        catch (Exception e)
        {
            exception = e;
            f = AddMachineKeySet(flags);
            ImportCertificate(collection, rawData, password, f);
        }

        LogIfNeeded(nameof(Import), f, exception);
        return;

        static void ImportCertificate(X509Certificate2Collection collection, byte[] data, string password, X509KeyStorageFlags keyStorageFlags)
        {
#if NET9_0_OR_GREATER
            collection.Add(X509CertificateLoader.LoadPkcs12(data, password, keyStorageFlags));
#else
            collection.Import(data, password, keyStorageFlags);
#endif
        }
    }

    public static X509Certificate2 CreateCertificate(byte[] rawData, string password = null, X509KeyStorageFlags? flags = null)
    {
#if NET9_0_OR_GREATER
        return CreateCertificate(f => X509CertificateLoader.LoadPkcs12(rawData, password, f), flags);
#else
        return CreateCertificate(f => new X509Certificate2(rawData, password, f), flags);
#endif
    }

    internal static X509Certificate2 CreateCertificate(string fileName, string password = null, X509KeyStorageFlags? flags = null)
    {
#if NET9_0_OR_GREATER
        return CreateCertificate(f => X509CertificateLoader.LoadPkcs12FromFile(fileName, password, f), flags);
#else
        return CreateCertificate(f => new X509Certificate2(fileName, password, f), flags);
#endif
    }

    private static X509Certificate2 CreateCertificate(Func<X509KeyStorageFlags, X509Certificate2> creator, X509KeyStorageFlags? flag)
    {
        DebugAssertDoesntContainKeySet(flag);
        var f = AddUserKeySet(flag);

        Exception exception = null;
        X509Certificate2 certificate;
        try
        {
            certificate = creator(f);
        }
        catch (Exception e)
        {
            exception = e;
            f = AddMachineKeySet(flag);
            certificate = creator(f);
        }

        LogIfNeeded(nameof(CreateCertificate), f, exception);

        CertificateCleaner.RegisterForDisposalDuringFinalization(certificate);

        return certificate;
    }

    private static X509KeyStorageFlags AddUserKeySet(X509KeyStorageFlags? flag)
    {
        return (flag ?? X509KeyStorageFlags.DefaultKeySet) | X509KeyStorageFlags.UserKeySet;
    }
    private static X509KeyStorageFlags AddMachineKeySet(X509KeyStorageFlags? flag)
    {
        return (flag ?? X509KeyStorageFlags.DefaultKeySet) | X509KeyStorageFlags.MachineKeySet;
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

    private static void LogIfNeeded(string method, X509KeyStorageFlags flags, Exception exception)
    {
        if (exception == null)
            return;

        if (FirstTime)
        {
            FirstTime = false;

            if (Logger.IsWarnEnabled)
                Logger.Warn(CreateMsg(), exception);
        }
        else
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug(CreateMsg(), exception);
        }

        return;

        string CreateMsg()
        {
            return $"{nameof(CertificateLoaderUtil)}.{method} - Flags used {flags}";
        }
    }

    private sealed class CertificateCleaner : CriticalFinalizerObject
    {
        private X509Certificate2 _certificate;
        private static readonly ConditionalWeakTable<X509Certificate2, CertificateCleaner> AssociateLifetimes = new();

        public static void RegisterForDisposalDuringFinalization(X509Certificate2 cert)
        {
            var cleaner = AssociateLifetimes.GetOrCreateValue(cert);
            cleaner!._certificate = cert;
        }

        ~CertificateCleaner() => _certificate?.Dispose();
    }
}
