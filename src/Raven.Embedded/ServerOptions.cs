using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Embedded
{
    public class ServerOptions
    {
        public string FrameworkVersion { get; set; } = "2.1.1";

        public string DataDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDB");

        internal string ServerDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "RavenDBServer");


        public TimeSpan MaxServerStartupTimeDuration { get; set; } = TimeSpan.FromMinutes(1);

        public List<string> CommandLineArgs { get; set; } = new List<string>();

        internal static ServerOptions Default = new ServerOptions();

        public SecurityOptions Security { get; private set; }

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
                ServerCertificiateThumbprint = cert.Thumbprint
            };

            return this;
        }

        public ServerOptions Secured(string certExec, string certExecArgs, string serverCertThumbprint, X509Certificate2 clientCert)
        {
            if (certExec == null)
                throw new ArgumentNullException(nameof(certExec));
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
                CertificateExec = certExec,
                CertificateArguments = certExecArgs,
                ServerCertificiateThumbprint = serverCertThumbprint
            };

            return this;
        }


        public class SecurityOptions
        {
            internal SecurityOptions() { }

            public string CertificatePath { get; internal set; }
            public string CertificatePassword { get; internal set; }
            public X509Certificate2 ClientCertificate { get; internal set; }
            public string CertificateExec { get; internal set; }
            public string CertificateArguments { get; internal set; }
            public string ServerCertificiateThumbprint { get; internal set; }
        }
    }


}
