using System;
using System.IO;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    public class ServerStatistics
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

        public DateTime? LastUserRequestTime;

        public void WriteTo(BlittableJsonTextWriter writer)
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

            writer.WritePropertyName(nameof(LastUserRequestTime));
            if (LastUserRequestTime.HasValue)
                writer.WriteDateTime(LastUserRequestTime.Value, isUtc: true);
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        internal unsafe void Load(TransactionContextPool contextPool, Logger logger)
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

                    using (var json = context.ReadForMemory(result.Reader.AsStream(), nameof(ServerStatistics)))
                    {
                        var stats = JsonDeserializationServer.ServerStatistics(json);

                        LastRequestTime = stats.LastRequestTime;
                        LastUserRequestTime = stats.LastUserRequestTime;
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
            lock (this)
            {
                try
                {
                    using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var ms = new MemoryStream())
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
