using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Properties;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal abstract class AbstractSmugglerHandlerProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractSmugglerHandlerProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        internal void ApplyBackwardCompatibility(DatabaseSmugglerOptionsServerSide options)
        {
            if (options == null)
                return;

            if (((options.OperateOnTypes & DatabaseItemType.DatabaseRecord) != 0)
                && (options.OperateOnDatabaseRecordTypes == DatabaseRecordItemType.None))
            {
                options.OperateOnDatabaseRecordTypes = DatabaseSmugglerOptions.DefaultOperateOnDatabaseRecordTypes;
            }

            if (RequestRouter.TryGetClientVersion(HttpContext, out var version) == false)
                return;

            if (version.Major != RavenVersionAttribute.Instance.MajorVersion)
                return;

#pragma warning disable 618
            if (version.Minor < 2 && options.OperateOnTypes.HasFlag(DatabaseItemType.Counters))
#pragma warning restore 618
            {
                options.OperateOnTypes |= DatabaseItemType.CounterGroups;
            }

            // only all 4.0 and 4.1 less or equal to 41006
            if (version.Revision < 70 || version.Revision > 41006)
                return;

            if (options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                options.OperateOnTypes |= DatabaseItemType.Attachments;
        }

        internal static Stream GetInputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey != null)
                return new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(options.EncryptionKey));

            return fileStream;
        }

        internal static Stream GetOutputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey == null)
                return fileStream;

            var key = options?.EncryptionKey;
            return new EncryptingXChaCha20Poly1305Stream(fileStream,
                Convert.FromBase64String(key));
        }

        internal static async ValueTask WriteSmugglerResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }
    }
}
