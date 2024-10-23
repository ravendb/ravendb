﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Embedded
{
    public class ServerOptions
    {
        internal static string DefaultServerDirectory = Path.Combine(AppContext.BaseDirectory, "RavenDBServer");

        internal static string AltServerDirectory = Path.Combine(AppContext.BaseDirectory, "bin", "RavenDBServer");

        public string FrameworkVersion { get; set; } = "9.0.0-rc.2.24473.5";

        public string LogsPath { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDB", "Logs");

        public string DataDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDB");

        public string ServerDirectory { get; set; } = DefaultServerDirectory;

        public string DotNetPath { get; set; } = "dotnet";

        public string ServerUrl { get; set; }

        public TimeSpan GracefulShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

        public TimeSpan MaxServerStartupTimeDuration { get; set; } = TimeSpan.FromMinutes(1);

        public List<string> CommandLineArgs { get; set; } = new List<string>();

        internal static ServerOptions Default = new ServerOptions();

        public SecurityOptions Security { get; private set; }

        public LicensingOptions Licensing { get; set; } = new();

        public ServerOptions Secured(string certificate, string certPassword = null)
        {
            if (certificate == null)
                throw new ArgumentNullException(nameof(certificate));

            if (Security != null)
                throw new InvalidOperationException("The security has already been setup for this ServerOptions object");

            var cert = new X509Certificate2(certificate, certPassword);
            Security = new SecurityOptions
            {
                CertificatePath = certificate,
                CertificatePassword = certPassword,
                ClientCertificate = cert,
                ServerCertificateThumbprint = cert.Thumbprint
            };

            return this;
        }

        public ServerOptions Secured(string certLoadExec, string certExecArgs, string serverCertThumbprint, X509Certificate2 clientCert)
        {
            if (certLoadExec == null)
                throw new ArgumentNullException(nameof(certLoadExec));
            if (certExecArgs == null)
                throw new ArgumentNullException(nameof(certExecArgs));
            if (serverCertThumbprint == null)
                throw new ArgumentNullException(nameof(serverCertThumbprint));
            if (clientCert == null)
                throw new ArgumentNullException(nameof(clientCert));

            if (Security != null)
                throw new InvalidOperationException("The security has already been setup for this ServerOptions object");

            Security = new SecurityOptions
            {
                ClientCertificate = clientCert,
                CertificateLoadExec = certLoadExec,
                CertificateLoadExecArguments = certExecArgs,
                ServerCertificateThumbprint = serverCertThumbprint
            };

            return this;
        }

        public sealed class SecurityOptions
        {
            internal SecurityOptions() { }

            public string CertificatePath { get; internal set; }
            public string CertificatePassword { get; internal set; }
            public X509Certificate2 ClientCertificate { get; internal set; }
            public string CertificateLoadExec { get; internal set; }
            public string CertificateLoadExecArguments { get; internal set; }
            public string ServerCertificateThumbprint { get; internal set; }
        }

        public sealed class LicensingOptions
        {
            public string License { get; set; }
            public string LicensePath { get; set; }
            public bool EulaAccepted { get; set; }
            public bool DisableAutoUpdate { get; set; }
            public bool DisableAutoUpdateFromApi { get; set; }
            public bool DisableLicenseSupportCheck { get; set; } = true;
            public bool ThrowOnInvalidOrMissingLicense { get; set; }
        }
    }
}
