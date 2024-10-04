using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.ServerWide
{
    public sealed class ServerStatistics
    {
        private static readonly TimeSpan PersistFrequency = TimeSpan.FromMinutes(15);

        private DateTime _lastPersist;

        public ServerStatistics()
        {
            StartUpTime = _lastPersist = SystemTime.UtcNow;
        }

        [JsonDeserializationIgnore]
        public TimeSpan UpTime => SystemTime.UtcNow - StartUpTime;

        [JsonDeserializationIgnore]
        public readonly DateTime StartUpTime;

        public DateTime? LastRequestTime;

        public DateTime? LastAuthorizedNonClusterAdminRequestTime;

        public Dictionary<string, DateTime> LastRequestTimePerCertificate = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UpdateLastCertificateRequestTime(string certificateThumbprint, DateTime requestTime)
        {
            if (certificateThumbprint == null)
                return;

            var lastRequestTimePerCertificate = LastRequestTimePerCertificate;
            if (lastRequestTimePerCertificate.TryGetValue(certificateThumbprint, out var oldRequestTime))
            {
                if (requestTime - oldRequestTime >= RequestRouter.LastRequestTimeUpdateFrequency)
                    lastRequestTimePerCertificate[certificateThumbprint] = requestTime;

                return;
            }

            LastRequestTimePerCertificate = new Dictionary<string, DateTime>(lastRequestTimePerCertificate)
            {
                [certificateThumbprint] = requestTime
            };
        }

        internal void RemoveLastAuthorizedCertificateRequestTime(List<string> certificateThumbprints)
        {
            if (certificateThumbprints == null || certificateThumbprints.Count == 0)
                return;

            var lastRequestTimePerCertificate = LastRequestTimePerCertificate;
            var newLastRequestTimePerCertificate = new Dictionary<string, DateTime>(lastRequestTimePerCertificate);
            foreach (var certificateThumbprint in certificateThumbprints)
                newLastRequestTimePerCertificate.Remove(certificateThumbprint);

            LastRequestTimePerCertificate = newLastRequestTimePerCertificate;
        }

        public void WriteTo<TWriter>(TWriter writer)
            where TWriter : IBlittableJsonTextWriter
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(UpTime));
            writer.WriteString(UpTime.ToString("c"));
            writer.WriteComma();

            writer.WritePropertyName(nameof(StartUpTime));
            writer.WriteDateTime(StartUpTime, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(LastRequestTime));
            if (LastRequestTime.HasValue)
                writer.WriteDateTime(LastRequestTime.Value, isUtc: true);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(LastAuthorizedNonClusterAdminRequestTime));
            if (LastAuthorizedNonClusterAdminRequestTime.HasValue)
                writer.WriteDateTime(LastAuthorizedNonClusterAdminRequestTime.Value, isUtc: true);
            else
                writer.WriteNull();
            writer.WriteComma();

            var lastCertificateRequestTime = LastRequestTimePerCertificate;
            writer.WritePropertyName(nameof(LastRequestTimePerCertificate));
            writer.WriteStartObject();
            var first = true;
            foreach (var kvp in lastCertificateRequestTime)
            {
                if (first == false)
                    writer.WriteComma();

                first = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteDateTime(kvp.Value, isUtc: true);
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        internal void Load(TransactionContextPool contextPool, Logger logger)
        {
            try
            {
                using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var tree = tx.InnerTransaction.ReadTree(nameof(ServerStatistics));
                    if (tree == null)
                        return;

                    var result = tree.Read(nameof(ServerStatistics));
                    if (result == null)
                        return;

                    using (var json = context.Sync.ReadForMemory(result.Reader.AsStream(), nameof(ServerStatistics)))
                    {
                        var stats = JsonDeserializationServer.ServerStatistics(json);

                        LastRequestTime = stats.LastRequestTime;
                        LastAuthorizedNonClusterAdminRequestTime = stats.LastAuthorizedNonClusterAdminRequestTime;
                    }
                }
            }
            catch (Exception e)
            {
                if (logger.IsInfoEnabled)
                    logger.Info("Could not load server statistics.", e);
            }
        }

        internal void Persist(TransactionContextPool contextPool, Logger logger)
        {
            if (contextPool == null)
                return;

            lock (this)
            {
                try
                {
                    using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            WriteTo(writer);
                            writer.Flush();

                            ms.Position = 0;

                            var tree = tx.InnerTransaction.CreateTree(nameof(ServerStatistics));
                            tree.Add(nameof(ServerStatistics), ms);

                            tx.Commit();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (logger.IsInfoEnabled)
                        logger.Info("Could not persist server statistics.", e);
                }
            }
        }

        internal void MaybePersist(TransactionContextPool contextPool, Logger logger)
        {
            var now = SystemTime.UtcNow;
            if (now - _lastPersist <= PersistFrequency)
                return;

            try
            {
                Persist(contextPool, logger);
            }
            finally
            {
                _lastPersist = now;
            }
        }
    }
}
