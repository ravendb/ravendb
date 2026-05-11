using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;

namespace Tests.Infrastructure
{
    /// <summary>Audit-log assertions for tests that exercise audit-emitting endpoints.</summary>
    public static class AuditLogTestHelper
    {
        private static readonly TimeSpan DefaultPollTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(200);

        /// <summary>Fresh temp directory for audit output. Not registered for test cleanup —
        /// LoggingSource.AuditLog is a process-static singleton that holds the file handle open
        /// across server restarts, which would break NewDataPath cleanup.</summary>
        public static string GetTempAuditLogPath()
        {
            var path = Path.Combine(Path.GetTempPath(), "RavenDB-AuditLog-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            return path;
        }

        /// <summary>customSettings entry enabling audit logging into <paramref name="auditLogPath"/>.</summary>
        public static Dictionary<string, string> BuildAuditLogSettings(string auditLogPath)
        {
            if (string.IsNullOrEmpty(auditLogPath))
                throw new ArgumentException("Audit log path must not be null or empty.", nameof(auditLogPath));

            Directory.CreateDirectory(auditLogPath);

            return new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.AuditLogPath)] = auditLogPath
            };
        }

        /// <summary>One-call setup: secured server + fresh audit dir + cluster-admin client cert.
        /// Audit logging needs a secured (HTTPS) server, so we cannot share one across tests.</summary>
        public static (string auditLogPath, X509Certificate2 adminCert) SetupAuditLoggedServer(RavenTestBase parent)
        {
            var auditLogPath = GetTempAuditLogPath();
            var certificates = parent.Certificates.SetupServerAuthentication(customSettings: BuildAuditLogSettings(auditLogPath));
            var adminCert = parent.Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);
            return (auditLogPath, adminCert);
        }

        /// <summary>Polls until a line matching <paramref name="predicate"/> appears, or throws on timeout.</summary>
        public static async Task<string> WaitForAuditLineAsync(string auditLogPath, Func<string, bool> predicate, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            var deadline = DateTime.UtcNow + (timeout ?? DefaultPollTimeout);
            List<string> lastSeen = null;

            while (DateTime.UtcNow < deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                lastSeen = ReadAuditLines(auditLogPath);
                var match = lastSeen.FirstOrDefault(predicate);
                if (match != null)
                    return match;

                await Task.Delay(PollInterval, cancellationToken).ConfigureAwait(false);
            }

            var contents = lastSeen == null || lastSeen.Count == 0
                ? "(no audit log files or all empty)"
                : string.Join(Environment.NewLine, lastSeen);

            throw new TimeoutException(
                $"Timed out after {(timeout ?? DefaultPollTimeout).TotalSeconds:F1}s waiting for a matching audit log line in '{auditLogPath}'.{Environment.NewLine}" +
                $"Audit log contents at timeout:{Environment.NewLine}{contents}");
        }

        /// <summary>Snapshot of every line in every *.log file in the directory.
        /// LoggingSource names files <c>{yyyy-MM-dd-HH-mm}.{seq:000}.log</c> and rotates on size/day,
        /// so we glob — pinning to a single filename is fragile.</summary>
        public static List<string> ReadAuditLines(string auditLogPath)
        {
            if (string.IsNullOrEmpty(auditLogPath))
                throw new ArgumentException("Audit log path must not be null or empty.", nameof(auditLogPath));
            if (Directory.Exists(auditLogPath) == false)
                throw new DirectoryNotFoundException($"Audit log directory '{auditLogPath}' does not exist. Misconfigured test — use BuildAuditLogSettings to create and wire the path.");

            var result = new List<string>();

            foreach (var file in Directory.EnumerateFiles(auditLogPath, "*.log", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                    using var reader = new StreamReader(stream);
                    string line;
                    while ((line = reader.ReadLine()) != null)
                        result.Add(line);
                }
                catch (IOException)
                {
                    // file locked or rotating — retry on next poll
                }
                catch (UnauthorizedAccessException)
                {
                    // same — transient
                }
            }

            return result;
        }
    }
}
