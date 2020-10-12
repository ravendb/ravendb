using System;
using System.IO;
using System.Runtime.Serialization;
using System.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

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

        public readonly Lazy<MyX509Certificate2> ServerCertificate;

        public readonly Lazy<MyX509Certificate2> ClientCertificate1;

        public readonly Lazy<MyX509Certificate2> ClientCertificate2;

        public readonly Lazy<MyX509Certificate2> ClientCertificate3;


        public class MyX509Certificate2 : X509Certificate2
        {
            public MyX509Certificate2()
                : base()
            {
            }

            public MyX509Certificate2(byte[] rawData)
                : base(rawData)
            {
            }

            public MyX509Certificate2(byte[] rawData, string password)
                : base(rawData, password)
            {
            }

            public MyX509Certificate2(byte[] rawData, SecureString password)
                : base(rawData, password)
            {
            }

            public MyX509Certificate2(byte[] rawData, string password, X509KeyStorageFlags keyStorageFlags)
                : base(rawData, password, keyStorageFlags)
            {
            }

            public MyX509Certificate2(byte[] rawData, SecureString password, X509KeyStorageFlags keyStorageFlags)
                : base(rawData, password, keyStorageFlags)
            {
            }

            public MyX509Certificate2(string fileName)
                : base(fileName)
            {
            }

            public MyX509Certificate2(string fileName, string password)
                : base(fileName, password)
            {
            }

            public MyX509Certificate2(string fileName, SecureString password)
                : base(fileName, password)
            {
            }


            public MyX509Certificate2(string fileName, string password, X509KeyStorageFlags keyStorageFlags)
                : base(fileName, password, keyStorageFlags)
            {
            }

            public MyX509Certificate2(string fileName, SecureString password, X509KeyStorageFlags keyStorageFlags)
                : base(fileName, password, keyStorageFlags)
            {
            }

            public MyX509Certificate2(X509Certificate certificate)
                : base(certificate)
            {
            }

            protected MyX509Certificate2(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
                throw new PlatformNotSupportedException();
            }

            public override void Reset()
            {
                Console.WriteLine("Certificate reset from: " + Environment.StackTrace);
                base.Reset();
            }

            protected override void Dispose(bool d)
            {
                Console.WriteLine($"Certificate disposed (dispose:{d}) from: " + Environment.StackTrace);
                base.Dispose(d);
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

            ServerCertificate = new Lazy<MyX509Certificate2>(() =>
            {
                try
                {
                    return new MyX509Certificate2(ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet);
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

            ServerCertificate = new Lazy<MyX509Certificate2>(() =>
            {
                try
                {
                    return new MyX509Certificate2(ServerCertificatePath, (string)null, X509KeyStorageFlags.MachineKeySet);
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

        private Lazy<MyX509Certificate2> CreateLazy(Func<string> path, int index)
        {
            return new Lazy<MyX509Certificate2>(() =>
            {
                try
                {
                    return new MyX509Certificate2(path(), (string)null, X509KeyStorageFlags.MachineKeySet);
                }
                catch (CryptographicException e)
                {
                    throw new CryptographicException($"Failed to load the test client certificate ({index}) from {path}.", e);
                }
            });
        }
    }
}
