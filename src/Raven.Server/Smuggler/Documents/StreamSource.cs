﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.Properties;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron;
using Constants = Raven.Client.Constants;
using Size = Sparrow.Size;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamSource : ISmugglerSource, IDisposable
    {
        private readonly PeepingTomStream _peepingTomStream;
        private readonly JsonOperationContext _context;
        private readonly RavenLogger _log;

        private JsonOperationContext.MemoryBuffer _buffer;
        private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
        private JsonParserState _state;
        private UnmanagedJsonParser _parser;
        private DatabaseItemType? _currentType;

        private SmugglerResult _result;

        private BuildVersionType _buildVersionType;
        private bool _readLegacyEtag;

        private Size _totalObjectsRead = new Size(0, SizeUnit.Bytes);
        private DatabaseItemType _operateOnTypes;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        protected readonly ByteStringContext _allocator;
        
        public StreamSource(Stream stream, JsonOperationContext context, string databaseName, DatabaseSmugglerOptionsServerSide options)
        {
            _peepingTomStream = new PeepingTomStream(stream, context);
            _context = context;
            _log = RavenLogManager.Instance.GetLoggerForDatabase<StreamSource>(databaseName);
            _options = options;
            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        }

        public async Task<SmugglerInitializeResult> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result)
        {
            _result = result;
            _returnBuffer = _context.GetMemoryBuffer(out _buffer);
            _state = new JsonParserState();
            _parser = new UnmanagedJsonParser(_context, _state, "file");

            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json.", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start object, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            _operateOnTypes = options.OperateOnTypes;
            var buildVersion = await ReadBuildVersionAsync();
            _buildVersionType = BuildVersion.Type(buildVersion);
            _readLegacyEtag = options.ReadLegacyEtag;

            var disposable = new DisposableAction(() =>
            {
                _parser.Dispose();
                _returnBuffer.Dispose();
            });

            return new SmugglerInitializeResult(disposable, buildVersion);
        }

        public async Task<DatabaseItemType> GetNextTypeAsync()
        {
            if (_currentType != null)
            {
                var currentType = _currentType.Value;
                _currentType = null;

                return currentType;
            }

            var type = await ReadTypeAsync();
            var dbItemType = GetType(type);
            while (dbItemType == DatabaseItemType.Unknown)
            {
                var msg = $"You are trying to import items of type '{type}' which is unknown or not supported in {RavenVersionAttribute.Instance.Version}. Ignoring items.";
                if (_log.IsWarnEnabled)
                    _log.Warn(msg);
                _result.AddWarning(msg);

                await SkipArrayAsync(onSkipped: null, MaySkipBlobAsync, CancellationToken.None);
                type = await ReadTypeAsync();
                dbItemType = GetType(type);
            }

            return dbItemType;
        }

        public async Task<DatabaseRecord> GetDatabaseRecordAsync()
        {
            var databaseRecord = new DatabaseRecord();
            await ReadObjectAsync(reader =>
            {
                if (reader.TryGet(nameof(databaseRecord.Revisions), out BlittableJsonReaderObject revisions) &&
                    revisions != null)
                {
                    try
                    {
                        databaseRecord.Revisions = JsonDeserializationCluster.RevisionsConfiguration(revisions);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the revisions configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.TimeSeries), out BlittableJsonReaderObject timeseries) &&
                    timeseries != null)
                {
                    try
                    {
                        databaseRecord.TimeSeries = JsonDeserializationCluster.TimeSeriesConfiguration(timeseries);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the timeseries configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.DocumentsCompression), out BlittableJsonReaderObject documentsCompression)
                    && documentsCompression != null)
                {
                    try
                    {
                        databaseRecord.DocumentsCompression = JsonDeserializationCluster.DocumentsCompressionConfiguration(documentsCompression);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the documents compression configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Expiration), out BlittableJsonReaderObject expiration) &&
                    expiration != null)
                {
                    try
                    {
                        databaseRecord.Expiration = JsonDeserializationCluster.ExpirationConfiguration(expiration);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the expiration configuration from smuggler file. Skipping.", e);
                    }
                }
                
                if (reader.TryGet(nameof(databaseRecord.DataArchival), out BlittableJsonReaderObject archival) &&
                    archival != null)
                {
                    try
                    {
                        databaseRecord.DataArchival = JsonDeserializationCluster.DataArchivalConfiguration(archival);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the archival configuration from smuggler file. Skipping.", e);
                    }
                }


                if (reader.TryGet(nameof(databaseRecord.Refresh), out BlittableJsonReaderObject refresh) &&
                    refresh != null)
                {
                    try
                    {
                        databaseRecord.Refresh = JsonDeserializationCluster.RefreshConfiguration(refresh);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the refresh configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.ConflictSolverConfig), out BlittableJsonReaderObject conflictSolverConfig) &&
                    conflictSolverConfig != null)
                {
                    try
                    {
                        databaseRecord.ConflictSolverConfig = JsonDeserializationCluster.ConflictSolverConfig(conflictSolverConfig);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the Conflict Solver Config configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.PeriodicBackups), out BlittableJsonReaderArray periodicBackups) &&
                    periodicBackups != null)
                {
                    databaseRecord.PeriodicBackups = new List<PeriodicBackupConfiguration>();
                    foreach (BlittableJsonReaderObject backup in periodicBackups)
                    {
                        try
                        {
                            var periodicBackup = JsonDeserializationCluster.PeriodicBackupConfiguration(backup);
                            databaseRecord.PeriodicBackups.Add(periodicBackup);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the periodic Backup configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Settings), out BlittableJsonReaderArray settings) &&
                    settings != null)
                {
                    databaseRecord.Settings = new Dictionary<string, string>();
                    foreach (BlittableJsonReaderObject config in settings)
                    {
                        try
                        {
                            var key = config.GetPropertyNames()[0];
                            config.TryGet(key, out string val);
                            databaseRecord.Settings[key] = val;
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the settings configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.UnusedDatabaseIds), out BlittableJsonReaderArray unusedDatabaseIds) &&
                    unusedDatabaseIds != null)
                {
                    foreach (var id in unusedDatabaseIds)
                    {
                        if (id is LazyStringValue == false && id is LazyCompressedStringValue == false)
                            throw new InvalidOperationException($"{nameof(databaseRecord.UnusedDatabaseIds)} should be a collection of strings but got {id.GetType()}");
                        databaseRecord.UnusedDatabaseIds.Add(id.ToString());
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.ExternalReplications), out BlittableJsonReaderArray externalReplications) &&
                    externalReplications != null)
                {
                    databaseRecord.ExternalReplications = new List<ExternalReplication>();
                    foreach (BlittableJsonReaderObject replication in externalReplications)
                    {
                        try
                        {
                            databaseRecord.ExternalReplications.Add(JsonDeserializationCluster.ExternalReplication(replication));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the External Replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Sorters), out BlittableJsonReaderObject sorters) &&
                    sorters != null)
                {
                    databaseRecord.Sorters = new Dictionary<string, SorterDefinition>();

                    try
                    {
                        foreach (var sorterName in sorters.GetPropertyNames())
                        {
                            if (sorters.TryGet(sorterName, out BlittableJsonReaderObject sorter) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the sorters {sorterName} from smuggler file. Skipping.");

                                continue;
                            }

                            databaseRecord.Sorters[sorterName] = JsonDeserializationServer.SorterDefinition(sorter);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the sorters configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Analyzers), out BlittableJsonReaderObject analyzers) &&
                    analyzers != null)
                {
                    databaseRecord.Analyzers = new Dictionary<string, AnalyzerDefinition>();

                    try
                    {
                        foreach (var analyzerName in analyzers.GetPropertyNames())
                        {
                            if (analyzers.TryGet(analyzerName, out BlittableJsonReaderObject analyzer) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the analyzer {analyzerName} from smuggler file. Skipping.");

                                continue;
                            }

                            databaseRecord.Analyzers[analyzerName] = JsonDeserializationServer.AnalyzerDefinition(analyzer);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the analyzers configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SinkPullReplications), out BlittableJsonReaderArray sinkPullReplications) &&
                    sinkPullReplications != null)
                {
                    databaseRecord.SinkPullReplications = new List<PullReplicationAsSink>();
                    foreach (BlittableJsonReaderObject pullReplication in sinkPullReplications)
                    {
                        try
                        {
                            var sink = JsonDeserializationCluster.PullReplicationAsSink(pullReplication);
                            databaseRecord.SinkPullReplications.Add(sink);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import sink pull replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.HubPullReplications), out BlittableJsonReaderArray hubPullReplications) &&
                    hubPullReplications != null)
                {
                    databaseRecord.HubPullReplications = new List<PullReplicationDefinition>();
                    foreach (BlittableJsonReaderObject pullReplication in hubPullReplications)
                    {
                        try
                        {
                            var hub = JsonDeserializationClient.PullReplicationDefinition(pullReplication);
                            databaseRecord.HubPullReplications.Add(hub);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info($"Wasn't able to import the pull replication configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.RavenEtls), out BlittableJsonReaderArray ravenEtls) &&
                    ravenEtls != null)
                {
                    databaseRecord.RavenEtls = new List<RavenEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in ravenEtls)
                    {
                        try
                        {
                            databaseRecord.RavenEtls.Add(JsonDeserializationCluster.RavenEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Raven Etls configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SqlEtls), out BlittableJsonReaderArray sqlEtls) &&
                    sqlEtls != null)
                {
                    databaseRecord.SqlEtls = new List<SqlEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in sqlEtls)
                    {
                        try
                        {
                            databaseRecord.SqlEtls.Add(JsonDeserializationCluster.SqlEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Raven SQL Etls configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.QueueSinks), out BlittableJsonReaderArray queueSinks) &&
                    queueSinks != null)
                {
                    databaseRecord.QueueSinks = new List<QueueSinkConfiguration>();
                    foreach (BlittableJsonReaderObject queue in queueSinks)
                    {
                        try
                        {
                            databaseRecord.QueueSinks.Add(JsonDeserializationCluster.QueueSinkConfiguration(queue));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Raven queue sinks configuration from smuggler file. Skipping.", e);
                        }
                    }
                }
                
                if (reader.TryGet(nameof(databaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject ravenConnectionStrings) &&
                    ravenConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in ravenConnectionStrings.GetPropertyNames())
                        {
                            if (ravenConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the RavenDB connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.RavenConnectionString(connection);
                            databaseRecord.RavenConnectionStrings[connectionName] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.RavenConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the RavenDB connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SqlConnectionStrings), out BlittableJsonReaderObject sqlConnectionStrings) &&
                    sqlConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in sqlConnectionStrings.GetPropertyNames())
                        {
                            if (sqlConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the SQL connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.SqlConnectionString(connection);
                            databaseRecord.SqlConnectionStrings[connectionString.Name] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.SqlConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the SQL connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Client), out BlittableJsonReaderObject client) &&
                    client != null)
                {
                    try
                    {
                        databaseRecord.Client = JsonDeserializationCluster.ClientConfiguration(client);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the client configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.LockMode), out string lockMode))
                {
                    try
                    {
                        databaseRecord.LockMode = Enum.Parse<DatabaseLockMode>(lockMode, ignoreCase: true);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the database lock mode from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.OlapEtls), out BlittableJsonReaderArray olapEtls) &&
                    olapEtls != null)
                {
                    databaseRecord.OlapEtls = new List<OlapEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in olapEtls)
                    {
                        try
                        {
                            databaseRecord.OlapEtls.Add(JsonDeserializationCluster.OlapEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the OLAP ETLs configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.OlapConnectionStrings), out BlittableJsonReaderObject olapConnectionStrings) &&
                    olapConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in olapConnectionStrings.GetPropertyNames())
                        {
                            if (olapConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the OLAP connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.OlapConnectionString(connection);
                            databaseRecord.OlapConnectionStrings[connectionName] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.OlapConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the OLAP connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.ElasticSearchEtls), out BlittableJsonReaderArray elasticEtls) &&
                    elasticEtls != null)
                {
                    databaseRecord.ElasticSearchEtls = new List<ElasticSearchEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in elasticEtls)
                    {
                        try
                        {
                            databaseRecord.ElasticSearchEtls.Add(JsonDeserializationCluster.ElasticSearchEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Elastic Search ETLs configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.ElasticSearchConnectionStrings), out BlittableJsonReaderObject elasticConnectionStrings) &&
                    elasticConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in elasticConnectionStrings.GetPropertyNames())
                        {
                            if (elasticConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the Elastic Search connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.ElasticSearchConnectionString(connection);
                            databaseRecord.ElasticSearchConnectionStrings[connectionName] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.ElasticSearchConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the Elastic Search connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.QueueEtls), out BlittableJsonReaderArray queueEtls) &&
                    queueEtls != null)
                {
                    databaseRecord.QueueEtls = new List<QueueEtlConfiguration>();
                    foreach (BlittableJsonReaderObject etl in queueEtls)
                    {
                        try
                        {
                            databaseRecord.QueueEtls.Add(JsonDeserializationCluster.QueueEtlConfiguration(etl));
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Wasn't able to import the Queue ETLs configuration from smuggler file. Skipping.", e);
                        }
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.QueueConnectionStrings), out BlittableJsonReaderObject queueConnectionStrings) &&
                    queueConnectionStrings != null)
                {
                    try
                    {
                        foreach (var connectionName in queueConnectionStrings.GetPropertyNames())
                        {
                            if (queueConnectionStrings.TryGet(connectionName, out BlittableJsonReaderObject connection) == false)
                            {
                                if (_log.IsInfoEnabled)
                                    _log.Info($"Wasn't able to import the Queue connection string {connectionName} from smuggler file. Skipping.");

                                continue;
                            }

                            var connectionString = JsonDeserializationCluster.QueueConnectionString(connection);
                            databaseRecord.QueueConnectionStrings[connectionName] = connectionString;
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.QueueConnectionStrings.Clear();
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the Queue connection strings from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Integrations), out BlittableJsonReaderObject integrations) &&
                    integrations != null)
                {

                    if (integrations.TryGet(nameof(databaseRecord.Integrations.PostgreSql), out BlittableJsonReaderObject postgreSqlConfig))
                    {
                        databaseRecord.Integrations ??= new IntegrationConfigurations();
                    }
                    try
                    {
                        databaseRecord.Integrations.PostgreSql = JsonDeserializationCluster.PostgreSqlConfiguration(postgreSqlConfig);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the PostgreSQL configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.IndexesHistory), out BlittableJsonReaderObject indexesHistory) && indexesHistory != null)
                {
                    try
                    {
                        databaseRecord.IndexesHistory = new();
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < indexesHistory.Count; i++)
                        {
                            indexesHistory.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderArray bjra)
                            {
                                var list = new List<IndexHistoryEntry>();
                                foreach (BlittableJsonReaderObject element in bjra)
                                    list.Add(JsonDeserializationCluster.IndexHistoryEntry(element));

                                databaseRecord.IndexesHistory[propertyDetails.Name] = list;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        databaseRecord.IndexesHistory = null; // skip when we hit a error.
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the IndexesHistory from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Studio), out BlittableJsonReaderObject studioConfig) &&
                    studioConfig != null)
                {
                    try
                    {
                        databaseRecord.Studio = JsonDeserializationCluster.StudioConfiguration(studioConfig);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the studio configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.RevisionsForConflicts), out BlittableJsonReaderObject revisionForConflicts) &&
                    revisionForConflicts != null)
                {
                    try
                    {
                        databaseRecord.RevisionsForConflicts = JsonDeserializationCluster.RevisionsCollectionConfiguration(revisionForConflicts);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the RevisionsForConflicts configuration from smuggler file. Skipping.", e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.Sharding), out BlittableJsonReaderObject sharding) &&
                    sharding != null)
                {
                    try
                    {
                        databaseRecord.Sharding = JsonDeserializationCluster.ShardingConfiguration(sharding);
                    }
                    catch (Exception e)
                    {
                        const string errorMessage = "Wasn't able to import Sharding configuration from smuggler file. Aborting.";

                        if (_log.IsInfoEnabled)
                        {
                            _log.Info(errorMessage, e);
                        }

                        throw new InvalidDataException(errorMessage, e);
                    }
                }

                if (reader.TryGet(nameof(databaseRecord.SupportedFeatures), out BlittableJsonReaderArray supportedFeaturesBjra) && supportedFeaturesBjra != null)
                {
                    try
                    {
                        var supportedFeatures = new List<string>();
                        foreach (var supportedFeature in supportedFeaturesBjra)
                            supportedFeatures.Add(supportedFeature.ToString());
                        databaseRecord.SupportedFeatures = supportedFeatures;
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Wasn't able to import the Supported Features from smuggler file. Skipping.", e);
                    }
                }
            });

            return databaseRecord;
        }

        public IAsyncEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesAsync()
        {
            return InternalGetCompareExchangeValuesAsync();
        }

        public IAsyncEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesAsync()
        {
            return InternalGetCompareExchangeTombstonesAsync();
        }

        public IAsyncEnumerable<CounterGroupDetail> GetCounterValuesAsync(List<string> collectionsToExport, ICounterActions actions)
        {
            return InternalGetCounterValuesAsync(actions);
        }

        public async IAsyncEnumerable<(string Hub, ReplicationHubAccess Access)> GetReplicationHubCertificatesAsync()
        {
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    if (reader.TryGet(nameof(RegisterReplicationHubAccessCommand.HubName), out string hub) == false)
                    {
                        _result.ReplicationHubCertificates.ErroredCount++;
                        _result.AddWarning("Could not read replication hub certificate entry.");

                        continue;
                    }

                    var access = JsonDeserializationClient.ReplicationHubAccess(reader);

                    yield return (hub, access);
                }
            }
        }

        public async IAsyncEnumerable<SubscriptionState> GetSubscriptionsAsync()
        {
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    if (reader.TryGet(nameof(SubscriptionState.SubscriptionName), out string subscriptionName) == false ||
                        reader.TryGet(nameof(SubscriptionState.Query), out string query) == false ||
                        reader.TryGet(nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint), out string changeVectorForNextBatchStartingPoint) == false ||
                        reader.TryGet(nameof(SubscriptionState.MentorNode), out string mentorNode) == false ||
                        reader.TryGet(nameof(SubscriptionState.NodeTag), out string nodeTag) == false ||
                        reader.TryGet(nameof(SubscriptionState.LastBatchAckTime), out DateTime lastBatchAckTime) == false ||
                        reader.TryGet(nameof(SubscriptionState.LastClientConnectionTime), out DateTime lastClientConnectionTime) == false ||
                        reader.TryGet(nameof(SubscriptionState.Disabled), out bool disabled) == false ||
                        reader.TryGet(nameof(SubscriptionState.SubscriptionId), out long subscriptionId) == false)
                    {
                        _result.Subscriptions.ErroredCount++;
                        _result.AddWarning("Could not read subscriptions entry.");

                        continue;
                    }

                    reader.TryGet(nameof(SubscriptionState.ArchivedDataProcessingBehavior), out ArchivedDataProcessingBehavior? archivedDataProcessingBehavior);
                    
                    yield return new SubscriptionState
                    {
                        Query = query,
                        ChangeVectorForNextBatchStartingPoint = changeVectorForNextBatchStartingPoint,
                        SubscriptionName = subscriptionName,
                        SubscriptionId = subscriptionId,
                        MentorNode = mentorNode,
                        NodeTag = nodeTag,
                        ArchivedDataProcessingBehavior = archivedDataProcessingBehavior,
                        LastBatchAckTime = lastBatchAckTime,
                        LastClientConnectionTime = lastClientConnectionTime,
                        Disabled = disabled
                    };
                }
            }
        }
        
        public async IAsyncEnumerable<TimeSeriesItem> GetTimeSeriesAsync(ITimeSeriesActions action, List<string> collectionsToOperate)
        {
            var collectionsHashSet = new HashSet<string>(collectionsToOperate, StringComparer.OrdinalIgnoreCase);

            await foreach (var reader in ReadArrayAsync(action))
            {
                if (reader.TryGet(Constants.Documents.Blob.Size, out int size) == false)
                        throw new InvalidOperationException($"Trying to read time series entry without size specified: doc: {reader}");

                if (reader.TryGet(Constants.Documents.Blob.Document, out BlittableJsonReaderObject blobMetadata) == false ||
                    blobMetadata.TryGet(nameof(TimeSeriesItem.Collection), out LazyStringValue collection) == false)
                {
                    await SkipEntryAsync(reader, size, skipDueToReadError: true);
                    continue;
                }

                if (collectionsHashSet.Count > 0 && collectionsHashSet.Contains(collection) == false)
                {
                    await SkipEntryAsync(reader, size, skipDueToReadError: false);
                    continue;
                }

                if (blobMetadata.TryGet(nameof(TimeSeriesItem.DocId), out LazyStringValue docId) == false ||
                    blobMetadata.TryGet(nameof(TimeSeriesItem.Name), out string name) == false ||
                    blobMetadata.TryGet(nameof(TimeSeriesItem.ChangeVector), out string cv) == false ||
                    blobMetadata.TryGet(nameof(TimeSeriesItem.Baseline), out DateTime baseline) == false)
                {
                    await SkipEntryAsync(reader, size, skipDueToReadError: true);
                    continue;
                }

                var segment = await ReadSegmentAsync(action, size);
                action.RegisterForDisposal(reader);
                
                yield return new TimeSeriesItem
                {
                    DocId = docId,
                    Name = name,
                    Baseline = baseline,
                    Collection = collection,
                    ChangeVector = cv,
                    Segment = segment,
                    SegmentSize = size
                };
            }
            
            async Task SkipEntryAsync(BlittableJsonReaderObject reader, int size, bool skipDueToReadError)
            {
                if (skipDueToReadError)
                {
                    _result.TimeSeries.ErroredCount++;
                    _result.AddWarning($"Could not read time series entry. {reader}");
                }
                
                reader.Dispose();
                await SkipAsync(size);
            }
        }

        private async Task<TimeSeriesValuesSegment> ReadSegmentAsync(ITimeSeriesActions action, int segmentSize)
        {
            var mem = action.GetContextForNewDocument().GetMemory(segmentSize);
            action.RegisterForReturnToTheContext(mem);
            var offset = 0;

            var size = segmentSize;
            while (size > 0)
            {
                (bool Done, int BytesRead) read;
                unsafe
                {
                    read = _parser.Copy(mem.Address + offset, size);
                }

                if (read.Done == false)
                {
                    offset += read.BytesRead;

                    var read2 = await _peepingTomStream.ReadAsync(_buffer.Memory.Memory);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }

                size -= read.BytesRead;
            }


            unsafe
            {
                return new TimeSeriesValuesSegment(mem.Address, segmentSize);
            }
        }

        private unsafe void SetBuffer(UnmanagedJsonParser parser, LazyStringValue value)
        {
            parser.SetBuffer(value.Buffer, value.Size);
        }

        private async IAsyncEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> InternalGetCompareExchangeValuesAsync()
        {
            using (_context.AcquireParserState(out var state))
            using (var parser = new UnmanagedJsonParser(_context, state, "Import/CompareExchange"))
            using (var builder = new BlittableJsonDocumentBuilder(_context,
                Mode, "Import/CompareExchange", parser, state))
            {
                await foreach (var reader in ReadArrayAsync())
                {
                    using (reader)
                    {
                        if (reader.TryGet("Key", out string key) == false ||
                            reader.TryGet("Value", out LazyStringValue value) == false)
                        {
                            _result.CompareExchange.ErroredCount++;
                            _result.AddWarning("Could not read compare exchange entry.");

                            continue;
                        }

                        using (value)
                        {
                            builder.ReadNestedObject();
                            SetBuffer(parser, value);
                            parser.Read();
                            builder.Read();
                            builder.FinalizeDocument();
                            yield return (new CompareExchangeKey(key), 0, builder.CreateReader());

                            builder.Renew("import/cmpxchg", Mode);
                        }
                    }
                }
            }
        }

        private async IAsyncEnumerable<(CompareExchangeKey Key, long Index)> InternalGetCompareExchangeTombstonesAsync()
        {
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    if (reader.TryGet("Key", out string key) == false)
                    {
                        _result.CompareExchange.ErroredCount++;
                        _result.AddWarning("Could not read compare exchange tombstone.");

                        continue;
                    }

                    yield return (new CompareExchangeKey(key), 0);
                }
            }
        }

        private async IAsyncEnumerable<CounterGroupDetail> InternalGetCounterValuesAsync(ICounterActions actions)
        {
            await foreach (var reader in ReadArrayAsync(actions))
            {
                if (reader.TryGet(nameof(CounterItem.DocId), out LazyStringValue docId) == false ||
                    reader.TryGet(nameof(CounterItem.Batch.Values), out BlittableJsonReaderObject values) == false ||
                    reader.TryGet(nameof(CounterItem.ChangeVector), out LazyStringValue cv) == false)
                {
                    _result.Counters.ErroredCount++;
                    _result.AddWarning("Could not read counter entry.");

                    continue;
                }

                values = ConvertToBlob(values, actions);
                actions.RegisterForDisposal(reader);

                yield return new CounterGroupDetail
                {
                    DocumentId = docId,
                    ChangeVector = cv,
                    Values = values
                };
            }
        }

        private unsafe BlittableJsonReaderObject ConvertToBlob(BlittableJsonReaderObject values, ICounterActions actions)
        {
            var scopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();
            try
            {
                var context = actions.GetContextForNewDocument();
                Debug.Assert(context == values._context);
                values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counterValues);

                counterValues.Modifications = new DynamicJsonValue(counterValues);
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < counterValues.Count; i++)
                {
                    counterValues.GetPropertyByIndex(i, ref prop);

                    if (prop.Value is LazyStringValue)
                        continue; //deleted counter

                    var arr = (BlittableJsonReaderArray)prop.Value;
                    var sizeToAllocate = CountersStorage.SizeOfCounterValues * arr.Length / 2;
                    scopes.Add(_allocator.Allocate(sizeToAllocate, out var newVal));

                    for (int j = 0; j < arr.Length; j += 2)
                    {
                        var newEntry = (CountersStorage.CounterValues*)newVal.Ptr + j / 2;
                        newEntry->Value = (long)arr[j];
                        newEntry->Etag = (long)arr[j + 1];
                    }

                    counterValues.Modifications[prop.Name] = new BlittableJsonReaderObject.RawBlob(newVal.Ptr, newVal.Length);
                }

                return context.ReadObject(values, null);
            }
            finally
            {
                foreach (var scope in scopes)
                {
                    scope.Dispose();
                }
            }
        }

        public async Task<long> SkipTypeAsync(DatabaseItemType type, Action<long> onSkipped, CancellationToken token)
        {
            switch (type)
            {
                case DatabaseItemType.None:
                    return 0;

                case DatabaseItemType.Documents:
                case DatabaseItemType.RevisionDocuments:
                case DatabaseItemType.Tombstones:
                case DatabaseItemType.Conflicts:
                case DatabaseItemType.Indexes:
                case DatabaseItemType.Identities:
                case DatabaseItemType.CompareExchange:
                case DatabaseItemType.Subscriptions:
                case DatabaseItemType.CompareExchangeTombstones:
                case DatabaseItemType.LegacyDocumentDeletions:
                case DatabaseItemType.LegacyAttachmentDeletions:
                case DatabaseItemType.CounterGroups:
                case DatabaseItemType.TimeSeriesDeletedRanges:
                case DatabaseItemType.ReplicationHubCertificates:
                    return await SkipArrayAsync(onSkipped, null, token);

                case DatabaseItemType.TimeSeries:
                    return await SkipArrayAsync(onSkipped, SkipBlobAsync, token);

                case DatabaseItemType.DatabaseRecord:
                    return await SkipObjectAsync(onSkipped);

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public SmugglerSourceType GetSourceType()
        {
            return SmugglerSourceType.Import;
        }

        public async IAsyncEnumerable<TimeSeriesDeletedRangeItemForSmuggler> GetTimeSeriesDeletedRangesAsync(ITimeSeriesActions action, List<string> collectionsToExport)
        {
            var collectionsHashSet = new HashSet<string>(collectionsToExport, StringComparer.OrdinalIgnoreCase);

            await foreach (var reader in ReadArrayAsync(action))
            {
                if (reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.Collection), out LazyStringValue collection) == false ||
                    reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.DocId), out LazyStringValue docId) == false ||
                    reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.Name), out LazyStringValue name) == false ||
                    reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.ChangeVector), out LazyStringValue cv) == false ||
                    reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.From), out DateTime from) == false ||
                    reader.TryGet(nameof(TimeSeriesDeletedRangeItemForSmuggler.To), out DateTime to) == false)
                {
                    _result.TimeSeriesDeletedRanges.ErroredCount++;
                    _result.AddWarning("Could not read timeseries deleted range entry.");
                    continue;
                }

                if (collectionsHashSet.Count > 0 && collectionsHashSet.Contains(collection) == false)
                    continue;

                action.RegisterForDisposal(reader);

                yield return new TimeSeriesDeletedRangeItemForSmuggler
                {
                    DocId = docId,
                    Name = name,
                    Collection = collection,
                    ChangeVector = cv,
                    From = from,
                    To = to
                };
            }
        }

        public IAsyncEnumerable<DocumentItem> GetDocumentsAsync(List<string> collectionsToOperate, INewDocumentActions actions)
        {
            return ReadDocumentsAsync(collectionsToOperate, actions);
        }

        public IAsyncEnumerable<DocumentItem> GetRevisionDocumentsAsync(List<string> collectionsToOperate, INewDocumentActions actions)
        {
            return ReadDocumentsAsync(collectionsToOperate, actions);
        }

        public IAsyncEnumerable<DocumentItem> GetLegacyAttachmentsAsync(INewDocumentActions actions)
        {
            return ReadLegacyAttachmentsAsync(actions);
        }

        public async IAsyncEnumerable<string> GetLegacyAttachmentDeletionsAsync()
        {
            await foreach (var id in ReadLegacyDeletionsAsync())
                yield return GetLegacyAttachmentId(id);
        }

        public IAsyncEnumerable<string> GetLegacyDocumentDeletionsAsync()
        {
            return ReadLegacyDeletionsAsync();
        }

        public IAsyncEnumerable<Tombstone> GetTombstonesAsync(List<string> collectionsToOperate, INewDocumentActions actions)
        {
            return ReadTombstonesAsync(collectionsToOperate, actions);
        }

        public IAsyncEnumerable<DocumentConflict> GetConflictsAsync(List<string> collectionsToOperate, INewDocumentActions actions)
        {
            return ReadConflictsAsync(collectionsToOperate, actions);
        }

        public async IAsyncEnumerable<IndexDefinitionAndType> GetIndexesAsync()
        {
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    IndexType type;
                    object indexDefinition;

                    try
                    {
                        indexDefinition = IndexProcessor.ReadIndexDefinition(reader, _buildVersionType, out type);
                    }
                    catch (Exception e)
                    {
                        _result.Indexes.ErroredCount++;
                        _result.AddWarning($"Could not read index definition. Message: {e.Message}");

                        continue;
                    }

                    yield return new IndexDefinitionAndType
                    {
                        Type = type,
                        IndexDefinition = indexDefinition
                    };
                }
            }
        }

        public IAsyncEnumerable<(string Prefix, long Value, long Index)> GetIdentitiesAsync()
        {
            return InternalGetIdentitiesAsync();
        }

        private async IAsyncEnumerable<(string Prefix, long Value, long Index)> InternalGetIdentitiesAsync()
        {
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    if (reader.TryGet("Key", out string identityKey) == false ||
                        reader.TryGet("Value", out string identityValueString) == false ||
                        long.TryParse(identityValueString, out long identityValue) == false)
                    {
                        _result.Identities.ErroredCount++;
                        _result.AddWarning("Could not read identity.");

                        continue;
                    }

                    yield return (identityKey, identityValue, 0);
                }
            }
        }

        private async Task<string> ReadTypeAsync()
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of object when reading type", _peepingTomStream, _parser);

            if (_state.CurrentTokenType == JsonParserToken.EndObject)
                return null;

            if (_state.CurrentTokenType != JsonParserToken.String)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected property type to be string, but was " + _state.CurrentTokenType, _peepingTomStream, _parser);

            unsafe
            {
                return _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize).ToString();
            }
        }

        private async Task ReadObjectAsync(BlittableJsonDocumentBuilder builder)
        {
            await UnmanagedJsonParserHelper.ReadObjectAsync(builder, _peepingTomStream, _parser, _buffer);

            _totalObjectsRead.Add(builder.SizeInBytes, SizeUnit.Bytes);
        }

        private async Task ReadObjectAsync(Action<BlittableJsonReaderObject> readAction)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start object, got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            using (var builder = CreateBuilder(_context))
            {
                _context.CachedProperties.NewDocument();
                await ReadObjectAsync(builder);

                using (var reader = builder.CreateReader())
                {
                    readAction(reader);
                }
            }
        }

        private async Task<long> ReadBuildVersionAsync()
        {
            var type = await ReadTypeAsync();
            if (type == null)
                return 0;

            if (type.Equals("BuildVersion", StringComparison.OrdinalIgnoreCase) == false)
            {
                _currentType = GetType(type);
                return 0;
            }

            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json.", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.Integer)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected integer BuildVersion, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            return _state.Long;
        }

        private async Task<long> SkipArrayAsync(Action<long> onSkipped, Func<BlittableJsonReaderObject, Task> additionalSkip, CancellationToken token)
        {
            var count = 0L;
            await foreach (var reader in ReadArrayAsync())
            {
                using (reader)
                {
                    token.ThrowIfCancellationRequested();

                    if (additionalSkip != null)
                        await additionalSkip(reader);

                    count++; //skipping
                    onSkipped?.Invoke(count);
                }
            }

            return count;
        }

        private Task SkipBlobAsync(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(Constants.Documents.Blob.Size, out int size) == false)
                throw new InvalidOperationException($"Trying to skip BLOB without size specified: doc: {reader}");

            return SkipAsync(size);
        }

        private async Task MaySkipBlobAsync(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(Constants.Documents.Blob.Size, out int size))
                await SkipAsync(size);
        }

        private Task SkipAttachmentStreamAsync(BlittableJsonReaderObject data)
        {
            if (data.TryGet(nameof(AttachmentName.Hash), out LazyStringValue _) == false ||
                data.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                data.TryGet(nameof(DocumentItem.AttachmentStream.Tag), out LazyStringValue _) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            return SkipAsync(size);
        }

        private async Task SkipAsync(long size)
        {
            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(32 * 1024, size);
                var read = _parser.Skip(sizeToRead);
                if (read.Done == false)
                {
                    var read2 = await _peepingTomStream.ReadAsync(_buffer.Memory.Memory);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");

                    _parser.SetBuffer(_buffer, 0, read2);
                }
                size -= read.BytesRead;
            }
        }

        private async Task<long> SkipObjectAsync(Action<long> onSkipped = null)
        {
            var count = 1;
            await ReadObjectAsync(reader => { });
            onSkipped?.Invoke(count);
            return count;
        }

        private async IAsyncEnumerable<BlittableJsonReaderObject> ReadArrayAsync(INewItemActions actions = null)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            BlittableJsonDocumentBuilder builder = null;

            try
            {
                while (true)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading array", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if ( actions != null)
                    {
                        context = actions.GetContextForNewDocument();
                        builder = actions.GetBuilderForNewDocument(_parser, _state);
                    }
                    else if (builder == null)
                            builder = CreateBuilder(context);

                    builder.Renew("import/object", Mode);

                    context.CachedProperties.NewDocument();

                    await ReadObjectAsync(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet(DocumentItem.ExportDocumentType.Key, out string type) &&
                        type == DocumentItem.ExportDocumentType.Attachment)
                    {
                        // skip document attachments, documents with attachments are handled separately
                        await SkipAttachmentStreamAsync(data);
                        continue;
                    }

                    yield return data;
                }
            }
            finally
            {
                if (actions == null)
                    builder?.Dispose();
            }
        }

        private async IAsyncEnumerable<string> ReadLegacyDeletionsAsync()
        {
            await foreach (var item in ReadArrayAsync())
            {
                if (item.TryGet("Key", out string key) == false)
                    continue;

                yield return key;
            }
        }

        private async IAsyncEnumerable<DocumentItem> ReadLegacyAttachmentsAsync(INewDocumentActions actions)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            BlittableJsonDocumentBuilder builder = null;
            BlittableMetadataModifier modifier = null;
            try
            {
                while (true)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading legacy attachments", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if(actions != null)
                    {
                        context = actions.GetContextForNewDocument();
                        modifier = actions.GetMetadataModifierForNewDocument();
                        builder = actions.GetBuilderForNewDocument(_parser, _state, modifier);
                    }
                    else if (builder == null)
                        {
                            modifier = new BlittableMetadataModifier(context);
                            builder = CreateBuilder(context, modifier);
                        }

                    builder.Renew("import/object", Mode);

                    context.CachedProperties.NewDocument();

                    await ReadObjectAsync(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var attachment = new DocumentItem.AttachmentStream
                    {
                        Stream = actions != null ? await actions.GetTempStreamAsync() : await GetTempStreamAsync()
                    };
                    var attachmentInfo = ProcessLegacyAttachment(context, data, ref attachment);
                    if (ShouldSkip(attachmentInfo))
                        continue;

                    var dummyDoc = new DocumentItem
                    {
                        Document = new Document
                        {
                            Data = WriteDummyDocumentForAttachment(context, attachmentInfo),
                            Id = attachmentInfo.Id,
                            ChangeVector = string.Empty,
                            Flags = DocumentFlags.HasAttachments,
                            NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler,
                            LastModified = SystemTime.UtcNow
                        },
                        Attachments = new List<DocumentItem.AttachmentStream>
                        {
                            attachment
                        }
                    };

                    yield return dummyDoc;
                }
            }
            finally
            {
                if (actions == null)
                {
                    builder?.Dispose();
                    modifier?.Dispose();
            }
        }
        }

        private static bool ShouldSkip(LegacyAttachmentDetails attachmentInfo)
        {
            if (attachmentInfo.Metadata.TryGet("Raven-Delete-Marker", out bool deleted) && deleted)
                return true;

            return attachmentInfo.Key.EndsWith(".deleting") || attachmentInfo.Key.EndsWith(".downloading");
        }

        public static BlittableJsonReaderObject WriteDummyDocumentForAttachment(JsonOperationContext context, LegacyAttachmentDetails details)
        {
            var attachment = new DynamicJsonValue
            {
                ["Name"] = details.Key,
                ["Hash"] = details.Hash,
                ["ContentType"] = string.Empty,
                ["Size"] = details.Size,
            };
            var attachments = new DynamicJsonArray();
            attachments.Add(attachment);
            var metadata = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = "@files",
                [Constants.Documents.Metadata.Attachments] = attachments,
                [Constants.Documents.Metadata.LegacyAttachmentsMetadata] = details.Metadata
            };
            var djv = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = metadata,
            };

            return context.ReadObject(djv, details.Id);
        }

        protected BlittableJsonDocumentBuilder.UsageMode Mode = BlittableJsonDocumentBuilder.UsageMode.ToDisk;

        private async IAsyncEnumerable<DocumentItem> ReadDocumentsAsync(List<string> collectionsToOperate, INewDocumentActions actions = null)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            var legacyImport = _buildVersionType == BuildVersionType.V3;
            BlittableMetadataModifier modifier = null;
            BlittableJsonDocumentBuilder builder = null;
            var collectionsHashSet = new HashSet<string>(collectionsToOperate, StringComparer.OrdinalIgnoreCase);

            try
            {
                List<DocumentItem.AttachmentStream> attachments = null;
                while (true)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        context = actions.GetContextForNewDocument();
                        modifier = actions.GetMetadataModifierForNewDocument(modifier?.FirstEtagOfLegacyRevision, modifier?.LegacyRevisionsCount ?? 0, legacyImport, _readLegacyEtag, _operateOnTypes);
                        builder = actions.GetBuilderForNewDocument(_parser, _state, modifier);
                    }
                    else if (builder == null)
                        {
                        modifier = new BlittableMetadataModifier(context, legacyImport, _readLegacyEtag, _operateOnTypes);
                            builder = CreateBuilder(context, modifier);
                        }

                    builder.Renew("import/object", Mode);

                    context.CachedProperties.NewDocument();

                    await ReadObjectAsync(builder);

                    var data = builder.CreateReader(); 
                    builder.Reset();

                    if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                    {
                        if (metadata.TryGet(Constants.Documents.Metadata.Collection, out string collectionName))
                        {
                            if (collectionsHashSet.Count > 0 && collectionsHashSet.Contains(collectionName) == false)
                            {
                                _result.Documents.SkippedCount++;
                                if (_result.Documents.SkippedCount % 1000 == 0)
                                    _result.AddInfo($"Skipped {_result.Documents.SkippedCount:#,#;;0} documents.");
                                continue;
                            }
                        }
                        
                        if (metadata.TryGet(DocumentItem.ExportDocumentType.Key, out string type))
                        {
                            if (type != DocumentItem.ExportDocumentType.Attachment)
                            {
                                var msg = $"Ignoring an item of type `{type}`. " + data;
                                if (_log.IsWarnEnabled)
                                    _log.Warn(msg);
                                _result.AddWarning(msg);
                                continue;
                            }

                            if (attachments == null)
                                attachments = new List<DocumentItem.AttachmentStream>();

                            var attachment = await ProcessAttachmentStreamAsync(context, data, actions);
                            attachments.Add(attachment);
                            continue;
                        }
                    }

                    if (legacyImport)
                    {
                        if (modifier.Id.Contains(HiLoHandler.RavenHiloIdPrefix))
                        {
                            data.Modifications = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = CollectionName.HiLoCollection
                                }
                            };
                        }
                    }

                    if (data.Modifications != null)
                    {
                        data = context.ReadObject(data, modifier.Id, Mode);
                    }

                    _result.LegacyLastDocumentEtag = modifier.LegacyEtag;

                    yield return new DocumentItem
                    {
                        Document = new Document
                        {
                            Data = data,
                            Id = context.GetLazyString(modifier.Id),
                            ChangeVector = modifier.ChangeVector,
                            Flags = modifier.Flags,
                            NonPersistentFlags = modifier.NonPersistentFlags,
                            LastModified = modifier.LastModified ?? SystemTime.UtcNow
                        },
                        Attachments = attachments
                    };
                    attachments = null;
                }
            }
            finally
            {
                if (actions == null)
                {
                    builder?.Dispose();
                    modifier?.Dispose();
            }
        }
        }

        public Task<Stream> GetTempStreamAsync() => StreamDestination.GetTempStreamAsync( _options);

        private async IAsyncEnumerable<Tombstone> ReadTombstonesAsync(List<string> collectionsToOperate, INewDocumentActions actions = null)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            BlittableJsonDocumentBuilder builder = null;
            var collectionsHashSet = new HashSet<string>(collectionsToOperate, StringComparer.OrdinalIgnoreCase);

            try
            {
                while (true)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        context = actions.GetContextForNewDocument();
                        builder = actions.GetBuilderForNewDocument(_parser, _state);
                    }
                    else if (builder == null)
                            builder = CreateBuilder(context);

                    builder.Renew("import/object", Mode);

                    _context.CachedProperties.NewDocument();

                    await ReadObjectAsync(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    var tombstone = new Tombstone();
                    if (data.TryGet(nameof(Tombstone.Collection), out tombstone.Collection))
                    {
                        if (collectionsHashSet.Count > 0 && collectionsHashSet.Contains(tombstone.Collection) == false)
                        {
                            continue;
                        }
                    }
                    else
                    {
                        SkipEntry(data);
                        continue;
                    }

                    if (data.TryGet("Key", out tombstone.LowerId) &&
                        data.TryGet(nameof(Tombstone.Type), out string type) &&
                        data.TryGet(nameof(Tombstone.LastModified), out tombstone.LastModified))
                    {
                        if (Enum.TryParse<Tombstone.TombstoneType>(type, out var tombstoneType) == false)
                        {
                            var msg = $"Ignoring a tombstone of type `{type}` which is not supported in 4.0. ";
                            if (_log.IsWarnEnabled)
                                _log.Warn(msg);

                            _result.Tombstones.ErroredCount++;
                            _result.AddWarning(msg);
                            continue;
                        }

                        tombstone.Type = tombstoneType;

                        if (data.TryGet(nameof(Tombstone.Flags), out string flags))
                        {
                            if (Enum.TryParse<DocumentFlags>(flags, out var tombstoneFlags) == false)
                            {
                                var msg = $"Ignoring a tombstone because it couldn't parse its flags: {flags}";
                                if (_log.IsWarnEnabled)
                                    _log.Warn(msg);

                                _result.Tombstones.ErroredCount++;
                                _result.AddWarning(msg);
                                continue;
                            }

                            tombstone.Flags = tombstoneFlags;
                        }

                        yield return tombstone;
                    }
                    else
                    {
                        SkipEntry(data);
                    }
                }
            }
            finally
            {
                if (actions == null)
                    builder?.Dispose();
            }

            void SkipEntry(BlittableJsonReaderObject data)
            {
                var msg = "Ignoring an invalid tombstone which you try to import. " + data;
                if (_log.IsWarnEnabled)
                    _log.Warn(msg);

                _result.Tombstones.ErroredCount++;
                _result.AddWarning(msg);
            }
        }

        private async IAsyncEnumerable<DocumentConflict> ReadConflictsAsync(List<string> collectionsToOperate, INewDocumentActions actions = null)
        {
            if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json", _peepingTomStream, _parser);

            if (_state.CurrentTokenType != JsonParserToken.StartArray)
                UnmanagedJsonParserHelper.ThrowInvalidJson("Expected start array, but got " + _state.CurrentTokenType, _peepingTomStream, _parser);

            var context = _context;
            BlittableJsonDocumentBuilder builder = null;
            var collectionsHashSet = new HashSet<string>(collectionsToOperate, StringComparer.OrdinalIgnoreCase);

            try
            {
                while (true)
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(_peepingTomStream, _parser, _state, _buffer) == false)
                        UnmanagedJsonParserHelper.ThrowInvalidJson("Unexpected end of json while reading docs", _peepingTomStream, _parser);

                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (actions != null)
                    {
                        context = actions.GetContextForNewDocument();
                        builder = actions.GetBuilderForNewDocument(_parser, _state);
                    }
                    else if (builder == null)
                            builder = CreateBuilder(context);

                    builder.Renew("import/object", Mode);

                    _context.CachedProperties.NewDocument();

                    await ReadObjectAsync(builder);

                    var data = builder.CreateReader();
                    builder.Reset();

                    if (data.TryGet(nameof(DocumentConflict.Collection), out string collectionName))
                    {
                        if (collectionsHashSet.Count > 0 && collectionsHashSet.Contains(collectionName) == false)
                            continue;
                    }
                    else
                    {
                        SkipEntry(data);
                        continue;
                    }

                    var conflict = new DocumentConflict();
                    if (data.TryGet(nameof(DocumentConflict.Id), out conflict.Id) &&
                        data.TryGet(nameof(DocumentConflict.Collection), out conflict.Collection) &&
                        data.TryGet(nameof(DocumentConflict.Flags), out string flags) &&
                        data.TryGet(nameof(DocumentConflict.ChangeVector), out conflict.ChangeVector) &&
                        data.TryGet(nameof(DocumentConflict.Etag), out conflict.Etag) &&
                        data.TryGet(nameof(DocumentConflict.LastModified), out conflict.LastModified) &&
                        data.TryGet(nameof(DocumentConflict.Doc), out conflict.Doc))
                    {
                        conflict.Flags = Enum.Parse<DocumentFlags>(flags);
                        if (conflict.Doc != null) // This is null for conflict that was generated from tombstone
                            conflict.Doc = context.ReadObject(conflict.Doc, conflict.Id, Mode);
                        yield return conflict;
                    }
                    else
                    {
                        SkipEntry(data);
                    }
                }
            }
            finally
            {
                if (actions == null)
                    builder?.Dispose();
            }

            void SkipEntry(BlittableJsonReaderObject data)
            {
                var msg = "Ignoring an invalid conflict which you try to import. " + data;
                if (_log.IsWarnEnabled)
                    _log.Warn(msg);

                _result.Conflicts.ErroredCount++;
                _result.AddWarning(msg);
            }
        }

        internal unsafe LegacyAttachmentDetails ProcessLegacyAttachment(
            JsonOperationContext context,
            BlittableJsonReaderObject data,
            ref DocumentItem.AttachmentStream attachment)
        {
            if (data.TryGet("Key", out string key) == false)
            {
                throw new ArgumentException("The key of legacy attachment is missing its key property.");
            }

            if (data.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
            {
                throw new ArgumentException($"Metadata of legacy attachment with key={key} is missing");
            }

            if (data.TryGet("Data", out string base64data) == false)
            {
                throw new ArgumentException($"Data of legacy attachment with key={key} is missing");
            }

            if (_readLegacyEtag && data.TryGet("Etag", out string etag))
            {
                _result.LegacyLastAttachmentEtag = etag;
            }

            var memoryStream = new MemoryStream();

            fixed (char* pdata = base64data)
            {
                memoryStream.SetLength(Base64.FromBase64_ComputeResultLength(pdata, base64data.Length));
                fixed (byte* buffer = memoryStream.GetBuffer())
                    Base64.FromBase64_Decode(pdata, base64data.Length, buffer, (int)memoryStream.Length);
            }

            memoryStream.Position = 0;

            return GenerateLegacyAttachmentDetails(context, memoryStream, key, metadata, _allocator, ref attachment);
        }

        public static string GetLegacyAttachmentId(string key)
        {
            return $"{DummyDocumentPrefix}{key}";
        }

        public static LegacyAttachmentDetails GenerateLegacyAttachmentDetails(
            JsonOperationContext context,
            Stream decodedStream,
            string key,
            BlittableJsonReaderObject metadata,
            ByteStringContext byteStringContext,
            ref DocumentItem.AttachmentStream attachment)
        {
            var stream = attachment.Stream;
            var hash = AsyncHelpers.RunSync(() => AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, decodedStream, stream, CancellationToken.None));
            attachment.Stream.Flush();
            var lazyHash = context.GetLazyString(hash);
            attachment.Base64HashDispose = Slice.External(byteStringContext, lazyHash, out attachment.Base64Hash);
            var tag = $"{DummyDocumentPrefix}{key}{RecordSeparator}d{RecordSeparator}{key}{RecordSeparator}{hash}{RecordSeparator}";
            var lazyTag = context.GetLazyString(tag);
            attachment.TagDispose = Slice.External(byteStringContext, lazyTag, out attachment.Tag);
            var id = GetLegacyAttachmentId(key);
            var lazyId = context.GetLazyString(id);

            attachment.Data = context.ReadObject(metadata, id);
            return new LegacyAttachmentDetails
            {
                Id = lazyId,
                Hash = hash,
                Key = key,
                Size = attachment.Stream.Length,
                Tag = tag,
                Metadata = attachment.Data
            };
        }

        public struct LegacyAttachmentDetails
        {
            public LazyStringValue Id;
            public string Hash;
            public string Key;
            public long Size;
            public string Tag;
            public BlittableJsonReaderObject Metadata;
        }

        private const char RecordSeparator = (char)SpecialChars.RecordSeparator;
        private const string DummyDocumentPrefix = "files/";

        public virtual async Task<DocumentItem.AttachmentStream> ProcessAttachmentStreamAsync(JsonOperationContext context, BlittableJsonReaderObject data,
            INewDocumentActions actions)
        {
            if (data.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                data.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                data.TryGet(nameof(DocumentItem.AttachmentStream.Tag), out LazyStringValue tag) == false)
                throw new ArgumentException($"Data of attachment stream is not valid: {data}");

            var attachment = new DocumentItem.AttachmentStream
            {
                Data = data
            };

            attachment.Base64HashDispose = Slice.External(_allocator, hash, out attachment.Base64Hash);
            attachment.TagDispose = Slice.External(_allocator, tag, out attachment.Tag);

            attachment.Stream = actions != null ? await actions.GetTempStreamAsync() : await GetTempStreamAsync();

            while (size > 0)
            {
                var sizeToRead = (int)Math.Min(_buffer.Size, size);

                (bool Done, int BytesRead) read = _parser.Copy(attachment.Stream, sizeToRead);

                if (read.Done == false)
                {
                    var read2 = await _peepingTomStream.ReadAsync(_buffer.Memory.Memory);
                    if (read2 == 0)
                        throw new EndOfStreamException("Stream ended without reaching end of stream content");


                    _parser.SetBuffer(_buffer, 0, read2);
                }

                size -= read.BytesRead;
            }

            await attachment.Stream.FlushAsync();


            return attachment;
        }

        private BlittableJsonDocumentBuilder CreateBuilder(JsonOperationContext context, BlittableMetadataModifier modifier = null)
        {
            return new BlittableJsonDocumentBuilder(context,
                Mode, "import/object", _parser, _state,
                modifier: modifier);
        }

        private static DatabaseItemType GetType(string type)
        {
            if (type == null)
                return DatabaseItemType.None;

            if (type.Equals(nameof(DatabaseItemType.DatabaseRecord), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.DatabaseRecord;

            if (type.Equals("Docs", StringComparison.OrdinalIgnoreCase) ||
                type.Equals("Results", StringComparison.OrdinalIgnoreCase)) // reading from stream/docs endpoint
                return DatabaseItemType.Documents;

            if (type.Equals(nameof(DatabaseItemType.RevisionDocuments), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.RevisionDocuments;

            if (type.Equals(nameof(DatabaseItemType.Tombstones), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Tombstones;

            if (type.Equals(nameof(DatabaseItemType.Conflicts), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Conflicts;

            if (type.Equals(nameof(DatabaseItemType.Indexes), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Indexes;

            if (type.Equals(nameof(DatabaseItemType.Identities), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Identities;

            if (type.Equals(nameof(DatabaseItemType.Subscriptions), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.Subscriptions;

            if (type.Equals(nameof(DatabaseItemType.CompareExchange), StringComparison.OrdinalIgnoreCase) ||
                type.Equals("CmpXchg", StringComparison.OrdinalIgnoreCase)) //support the old name
                return DatabaseItemType.CompareExchange;

            if (type.Equals(nameof(DatabaseItemType.CounterGroups), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.CounterGroups;

            if (type.Equals(nameof(DatabaseItemType.TimeSeries), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.TimeSeries;

            if (type.Equals(nameof(DatabaseItemType.CompareExchangeTombstones), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.CompareExchangeTombstones;

            if (type.Equals(nameof(DatabaseItemType.ReplicationHubCertificates), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.ReplicationHubCertificates;

            if (type.Equals("Attachments", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyAttachments;

            if (type.Equals("DocsDeletions", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyDocumentDeletions;

            if (type.Equals("AttachmentsDeletions", StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.LegacyAttachmentDeletions;

            if (type.Equals(nameof(DatabaseItemType.TimeSeriesDeletedRanges), StringComparison.OrdinalIgnoreCase))
                return DatabaseItemType.TimeSeriesDeletedRanges;

            return DatabaseItemType.Unknown;
        }

        public virtual Stream GetAttachmentStream(LazyStringValue hash, out string tag)
        {
            tag = null;
            return null;
        }

        public virtual void Dispose()
        {
            _peepingTomStream.Dispose();
            _allocator.Dispose();
        }
    }
}
