using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSmuggler : SmugglerBase
    {
        private readonly DocumentDatabase _database;

        public const string PreV4RevisionsDocumentId = "/revisions/";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPreV4Revision(BuildVersionType buildType, string id, Document document)
        {
            if (buildType == BuildVersionType.V3 == false)
                return false;

            if ((document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) != NonPersistentDocumentFlags.LegacyRevision)
                return false;

            return id.Contains(PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
        }

        public DatabaseSmuggler(string databaseName, DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time, JsonOperationContext context, DatabaseSmugglerOptionsServerSide options = null, SmugglerResult result = null, Action<IOperationProgress> onProgress = null, CancellationToken token = default) :
            base(databaseName, source, destination, time, context, options, result, onProgress, token)
        {
            _database = database;
            Debug.Assert((source is DatabaseSource && destination is DatabaseDestination) == false,
                "When both source and destination are database, we might get into a delayed write for the dest while the " +
                "source already pulsed its' read transaction, resulting in bad memory read.");
        }

        public override SmugglerPatcher CreatePatcher() => new DatabaseSmugglerPatcher(_options, _database);

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result, string databaseName)
        {
            result.CompareExchange.Start();
            await using (var actions = _destination.CompareExchange(databaseName, _context, BackupKind, withDocuments: false))
            {
                await foreach (var kvp in _source.GetCompareExchangeValuesAsync())
                {
                    await InternalProcessCompareExchangeAsync(result, kvp, actions);
                }
            }

            return result.CompareExchange;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result, string databaseName)
        {
            result.CompareExchangeTombstones.Start();

            await using (var actions = _destination.CompareExchangeTombstones(databaseName, _context))
            {
                await foreach (var key in _source.GetCompareExchangeTombstonesAsync())
                {
                    await InternalProcessCompareExchangeTombstonesAsync(result, key, actions);
                }
            }

            return result.CompareExchangeTombstones;
        }

        protected virtual async Task InternalProcessCompareExchangeAsync(SmugglerResult result, (CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value) kvp,
            ICompareExchangeActions actions)
        {
            _token.ThrowIfCancellationRequested();
            result.CompareExchange.ReadCount++;
            if (result.CompareExchange.ReadCount != 0 && result.CompareExchange.ReadCount % 1000 == 0)
                AddInfoToSmugglerResult(result, $"Read {result.CompareExchange.ReadCount:#,#;;0} compare exchange values.");

            if (kvp.Equals(default))
            {
                result.CompareExchange.ErroredCount++;
                return;
            }

            try
            {
                await actions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value, null);
                result.CompareExchange.LastEtag = kvp.Index;
            }
            catch (Exception e)
            {
                result.CompareExchange.ErroredCount++;
                result.AddError($"Could not write compare exchange with key: '{kvp.Key.Key}': {e.Message}");
            }
        }

        protected virtual async Task InternalProcessCompareExchangeTombstonesAsync(SmugglerResult result, (CompareExchangeKey Key, long Index) key, ICompareExchangeActions actions)
        {
            _token.ThrowIfCancellationRequested();
            result.CompareExchangeTombstones.ReadCount++;

            if (key.Equals(default))
            {
                result.CompareExchangeTombstones.ErroredCount++;
                return;
            }

            try
            {
                await actions.WriteTombstoneKeyAsync(key.Key.Key);
            }
            catch (Exception e)
            {
                result.CompareExchangeTombstones.ErroredCount++;
                result.AddError($"Could not write compare exchange '{key}: {e.Message}");
            }
        }
    }
}
