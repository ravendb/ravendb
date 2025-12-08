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
#pragma warning disable SYSLIB0057
            collection.Import(rawData, password, f);
#pragma warning restore SYSLIB0057
        }
#pragma warning disable CS0168 // Variable is declared but never used
        catch (Exception e)
#pragma warning restore CS0168 // Variable is declared but never used
        {
#if NET9_0_OR_GREATER
            throw;
#else
            exception = e;
            f = AddMachineKeySet(flags);
            collection.Import(rawData, password, f);
#endif
        }

        LogIfNeeded(nameof(Import), f, exception);
    }

    public static X509Certificate2 CreateCertificate(byte[] rawData, string password = null, X509KeyStorageFlags? flags = null)
    {
#pragma warning disable SYSLIB0057
        return CreateCertificate(f => new X509Certificate2(rawData, password, f), flags);
#pragma warning restore SYSLIB0057
    }

    internal static X509Certificate2 CreateCertificate(string fileName, string password = null, X509KeyStorageFlags? flags = null)
    {
#pragma warning disable SYSLIB0057
        return CreateCertificate(f => new X509Certificate2(fileName, password, f), flags);
#pragma warning restore SYSLIB0057
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
#pragma warning disable CS0168 // Variable is declared but never used
        catch (Exception e)
#pragma warning restore CS0168 // Variable is declared but never used
        {
#if NET9_0_OR_GREATER
            throw;
#else
            exception = e;
            f = AddMachineKeySet(flag);
            certificate = creator(f);
#endif
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
#if !NET9_0_OR_GREATER
            X509KeyStorageFlags.MachineKeySet |
#endif
            X509KeyStorageFlags.UserKeySet;

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
