﻿using System;
using System.Diagnostics;
using System.IO;
using System.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Util;

namespace FastTests
{
    public class TestCertificatesHolder
    {
        private readonly Func<string> _getServerCertificatePath;

        private readonly Func<string> _getClientCertificate1Path;

        private readonly Func<string> _getClientCertificate2Path;

        private readonly Func<string> _getClientCertificate3Path;

        private string _serverCertificatePath;

        private string _clientCertificate1Path;

        private string _clientCertificate2Path;

        private string _clientCertificate3Path;

        public readonly Lazy<TrackingX509Certificate2> ServerCertificate;

        public readonly Lazy<TrackingX509Certificate2> ClientCertificate1;

        public readonly Lazy<TrackingX509Certificate2> ClientCertificate2;

        public readonly Lazy<TrackingX509Certificate2> ClientCertificate3;

        public sealed class TrackingX509Certificate2 : X509Certificate2
        {
#pragma warning disable SYSLIB0026 // Type or member is obsolete
            public TrackingX509Certificate2()
            {
            }
#pragma warning restore SYSLIB0026 // Type or member is obsolete

#pragma warning disable SYSLIB0057
            public TrackingX509Certificate2(byte[] rawData)
                : base(rawData)
            {
            }

            public TrackingX509Certificate2(byte[] rawData, string password)
                : base(rawData, password)
            {
            }

            public TrackingX509Certificate2(byte[] rawData, SecureString password)
                : base(rawData, password)
            {
            }

            public TrackingX509Certificate2(byte[] rawData, string password, X509KeyStorageFlags keyStorageFlags)
                : base(rawData, password, keyStorageFlags)
            {
            }

            public TrackingX509Certificate2(byte[] rawData, SecureString password, X509KeyStorageFlags keyStorageFlags)
                : base(rawData, password, keyStorageFlags)
            {
            }

            public TrackingX509Certificate2(string fileName)
                : base(fileName)
            {
            }

            public TrackingX509Certificate2(string fileName, string password)
                : base(fileName, password)
            {
            }

            public TrackingX509Certificate2(string fileName, SecureString password)
                : base(fileName, password)
            {
            }

            public TrackingX509Certificate2(string fileName, string password, X509KeyStorageFlags keyStorageFlags)
                : base(fileName, password, keyStorageFlags)
            {
            }

            public TrackingX509Certificate2(string fileName, SecureString password, X509KeyStorageFlags keyStorageFlags)
                : base(fileName, password, keyStorageFlags)
            {
            }
#pragma warning restore SYSLIB0057

            public TrackingX509Certificate2(X509Certificate certificate)
                : base(certificate)
            {
            }

            public override void Reset()
            {
                Debug.Assert(false,"Do not dispose or reset this certificate.");
                Console.WriteLine("Certificate reset from: " + Environment.StackTrace);
                base.Reset();
            }
        }

        public string ServerCertificatePath
        {
            get
            {
                if (_serverCertificatePath == null)
                    _serverCertificatePath = _getServerCertificatePath();

                return _serverCertificatePath;
            }
        }

        public string ClientCertificate1Path
        {
            get
            {
                if (_clientCertificate1Path == null)
                    _clientCertificate1Path = _getClientCertificate1Path();

                return _clientCertificate1Path;
            }
        }

        public string ClientCertificate2Path
        {
            get
            {
                if (_clientCertificate2Path == null)
                    _clientCertificate2Path = _getClientCertificate2Path();

                return _clientCertificate2Path;
            }
        }

        public string ClientCertificate3Path
        {
            get
            {
                if (_clientCertificate3Path == null)
                    _clientCertificate3Path = _getClientCertificate3Path();

                return _clientCertificate3Path;
            }
        }

        public TestCertificatesHolder(string serverCertificatePath, string clientCertificate1Path, string clientCertificate2Path, string clientCertificate3Path)
        {
            _getServerCertificatePath = () => serverCertificatePath;
            _getClientCertificate1Path = () => clientCertificate1Path;
            _getClientCertificate2Path = () => clientCertificate2Path;
            _getClientCertificate3Path = () => clientCertificate3Path;

            ServerCertificate = new Lazy<TrackingX509Certificate2>(() =>
            {
                try
                {
                    return new TrackingX509Certificate2(ServerCertificatePath, (string)null, X509KeyStorageFlags.UserKeySet | CertificateLoaderUtil.FlagsForExport);
                }
                catch (CryptographicException e)
                {
                    throw new CryptographicException($"Failed to load the test server certificate from {ServerCertificatePath}.", e);
                }
            });

            ClientCertificate1 = CreateLazy(() => ClientCertificate1Path, 1);
            ClientCertificate2 = CreateLazy(() => ClientCertificate2Path, 2);
            ClientCertificate3 = CreateLazy(() => ClientCertificate3Path, 3);
        }

        public TestCertificatesHolder(TestCertificatesHolder parent, Func<string> getTemporaryFileName)
        {
            _getServerCertificatePath = () =>
            {
                var path = getTemporaryFileName();
                File.Copy(parent.ServerCertificatePath, path, true);

                return path;
            };

            _getClientCertificate1Path = () =>
            {
                var path = getTemporaryFileName();
                File.Copy(parent.ClientCertificate1Path, path, true);

                return path;
            };

            _getClientCertificate2Path = () =>
            {
                var path = getTemporaryFileName();
                File.Copy(parent.ClientCertificate2Path, path, true);

                return path;
            };

            _getClientCertificate3Path = () =>
            {
                var path = getTemporaryFileName();
                File.Copy(parent.ClientCertificate3Path, path, true);

                return path;
            };

            ServerCertificate = new Lazy<TrackingX509Certificate2>(() =>
            {
                try
                {
                    return new TrackingX509Certificate2(ServerCertificatePath, (string)null, X509KeyStorageFlags.UserKeySet | CertificateLoaderUtil.FlagsForExport);
                }
                catch (CryptographicException e)
                {
                    throw new CryptographicException($"Failed to load the test server certificate from {ServerCertificatePath}.", e);
                }
            });

            ClientCertificate1 = CreateLazy(() => ClientCertificate1Path, 1);
            ClientCertificate2 = CreateLazy(() => ClientCertificate2Path, 2);
            ClientCertificate3 = CreateLazy(() => ClientCertificate3Path, 3);
        }

        private Lazy<TrackingX509Certificate2> CreateLazy(Func<string> path, int index)
        {
            return new Lazy<TrackingX509Certificate2>(() =>
            {
                try
                {
                    return new TrackingX509Certificate2(path(), (string)null, X509KeyStorageFlags.UserKeySet | CertificateLoaderUtil.FlagsForExport);
                }
                catch (CryptographicException e)
                {
                    throw new CryptographicException($"Failed to load the test client certificate ({index}) from {path}.", e);
                }
            });
        }
    }
}
