using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly DocumentsOperationContext _context;
        private readonly DatabaseSource _source;
        private BlittableJsonTextWriter _writer;
        private static DatabaseSmugglerOptions _options;

        public StreamDestination(Stream stream, DocumentsOperationContext context, DatabaseSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new BlittableJsonTextWriter(_context, _gzipStream);
            _options = options;

            _writer.WriteStartObject();

            _writer.WritePropertyName("BuildVersion");
            _writer.WriteInteger(buildVersion);

            return new DisposableAction(() =>
            {
                _writer.WriteEndObject();
                _writer.Dispose();
                _gzipStream.Dispose();
            });
        }

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_writer, _context);
        }

        public IDocumentActions Documents()
        {
            return new StreamDocumentActions(_writer, _context, _source, "Docs", new SmugglerMetadataModifier(_options.OperateOnTypes));
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.RevisionDocuments));
        }

        public IDocumentActions Tombstones()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.Tombstones));
        }

        public IDocumentActions Conflicts()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.Conflicts));
        }

        public IKeyValueActions<long> Identities()
        {
            return new StreamKeyValueActions<long>(_writer, nameof(DatabaseItemType.Identities));
        }

        public IKeyValueActions<BlittableJsonReaderObject> CompareExchange(JsonOperationContext context)
        {
            return new StreamKeyValueActions<BlittableJsonReaderObject>(_writer, nameof(DatabaseItemType.CompareExchange));
        }

        public ICounterActions Counters()
        {
            return new StreamCounterActions(_writer, nameof(DatabaseItemType.Counters));
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        private class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly BlittableJsonTextWriter _writer;
            private readonly JsonOperationContext _context;

            public DatabaseRecordActions(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                _writer = writer;
                _context = context;

                _writer.WriteComma();
                _writer.WritePropertyName(nameof(DatabaseItemType.DatabaseRecord));
                _writer.WriteStartObject();
            }

            public void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType)
            {
                _writer.WritePropertyName(nameof(databaseRecord.DatabaseName));
                _writer.WriteString(databaseRecord.DatabaseName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Encrypted));
                _writer.WriteBool(databaseRecord.Encrypted);

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.ConflictSolverConfig))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.ConflictSolverConfig));
                    WriteConflictSolver(databaseRecord.ConflictSolverConfig);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Settings))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Settings));
                    WriteSettings(databaseRecord.Settings);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Revisions))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Revisions));
                    WriteRevisions(databaseRecord.Revisions);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Expiration))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Expiration));
                    WriteExpiration(databaseRecord.Expiration);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Client))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Client));
                    WriteClientConfiguration(databaseRecord.Client);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Sorters))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Sorters));
                    WriteSorters(databaseRecord.Sorters);
                }

                switch (authorizationStatus)
                {
                    case AuthorizationStatus.DatabaseAdmin:
                    case AuthorizationStatus.Operator:
                    case AuthorizationStatus.ClusterAdmin:
                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.RavenConnectionStrings));
                            WriteRavenConnectionStrings(databaseRecord.RavenConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SqlConnectionStrings));
                            WriteSqlConnectionStrings(databaseRecord.SqlConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.PeriodicBackups))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.PeriodicBackups));
                            WritePeriodicBackups(databaseRecord.PeriodicBackups);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.ExternalReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.ExternalReplications));
                            WriteExternalReplications(databaseRecord.ExternalReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.RavenEtls));
                            WriteRavenEtls(databaseRecord.RavenEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SqlEtls));
                            WriteSqlEtls(databaseRecord.SqlEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.HubPullReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.HubPullReplications));
                            WriteHubPullReplications(databaseRecord.HubPullReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SinkPullReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SinkPullReplications));
                            WriteSinkPullReplications(databaseRecord.SinkPullReplications);
                        }

                        break;
                }
            }

            private void WriteHubPullReplications(List<PullReplicationDefinition> hubPullReplications)
            {
                if (hubPullReplications == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();
                var first = true;
                foreach (var pullReplication in hubPullReplications)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, pullReplication.ToJson());
                }
                _writer.WriteEndArray();
            }

            private void WriteSinkPullReplications(List<PullReplicationAsSink> sinkPullReplications)
            {
                if (sinkPullReplications == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();
                var first = true;
                foreach (var pullReplication in sinkPullReplications)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, pullReplication.ToJson());

                }
                _writer.WriteEndArray();
            }

            private void WriteSorters(Dictionary<string, SorterDefinition> sorters)
            {
                if (sorters == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();
                var first = true;
                foreach (var sorter in sorters)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(sorter.Key);
                    _writer.WriteStartObject();

                    _writer.WritePropertyName(nameof(sorter.Value.Name));
                    _writer.WriteString(sorter.Value.Name);
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(sorter.Value.Code));
                    _writer.WriteString(sorter.Value.Code);

                    _writer.WriteEndObject();
                }

                _writer.WriteEndObject();
            }

            private static readonly HashSet<string> DoNotBackUp = new HashSet<string>
            {
                RavenConfiguration.GetKey(x => x.Core.DataDirectory),
                RavenConfiguration.GetKey(x => x.Storage.TempPath),
                RavenConfiguration.GetKey(x => x.Indexing.TempPath),
                RavenConfiguration.GetKey(x => x.Licensing.License),
                RavenConfiguration.GetKey(x => x.Core.RunInMemory)
            };

            private static readonly HashSet<string> ServerWideKeys = DatabaseHelper.GetServerWideOnlyConfigurationKeys().ToHashSet();

            private void WriteSettings(Dictionary<string, string> settings)
            {
                if (settings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartArray();
                var first = true;
                foreach (var config in settings)
                {
                    if (!(DoNotBackUp.Contains(config.Key, StringComparer.OrdinalIgnoreCase) || 
                          ServerWideKeys.Contains(config.Key, StringComparer.OrdinalIgnoreCase)))
                    {
                        if (first == false)
                            _writer.WriteComma();
                        first = false;
                        _writer.WriteStartObject();
                        _writer.WritePropertyName(config.Key);
                        _writer.WriteString(config.Value);
                        _writer.WriteEndObject();
                    }
                }
                _writer.WriteEndArray();
            }

            private void WriteSqlEtls(List<SqlEtlConfiguration> sqlEtlConfiguration)
            {
                if (sqlEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in sqlEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WriteStartObject();

                    _writer.WritePropertyName(nameof(etl.TaskId));
                    _writer.WriteDouble(etl.TaskId);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Name));
                    _writer.WriteString(etl.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.ConnectionStringName));
                    _writer.WriteString(etl.ConnectionStringName);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Transforms));
                    WriteTransforms(etl.Transforms);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Disabled));
                    _writer.WriteBool(etl.Disabled);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.FactoryName));
                    _writer.WriteString(etl.FactoryName);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.ParameterizeDeletes));
                    _writer.WriteBool(etl.ParameterizeDeletes);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.ForceQueryRecompile));
                    _writer.WriteBool(etl.ForceQueryRecompile);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.QuoteTables));
                    _writer.WriteBool(etl.QuoteTables);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.AllowEtlOnNonEncryptedChannel));
                    _writer.WriteBool(etl.AllowEtlOnNonEncryptedChannel);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.CommandTimeout));
                    if (etl.CommandTimeout != null)
                        _writer.WriteInteger(etl.CommandTimeout.Value);
                    else
                        _writer.WriteNull();
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.SqlTables));
                    WriteSqlTables(etl.SqlTables);
                    _writer.WriteEndObject();
                }

                _writer.WriteEndArray();
            }

            private void WriteSqlTables(List<SqlEtlTable> SqlTables)
            {
                if (SqlTables == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();
                var first = true;
                foreach (var sqlTable in SqlTables)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, sqlTable.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteRavenEtls(List<RavenEtlConfiguration> ravenEtlConfiguration)
            {
                if (ravenEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in ravenEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WriteStartObject();

                    _writer.WritePropertyName(nameof(etl.TaskId));
                    _writer.WriteDouble(etl.TaskId);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Name));
                    _writer.WriteString(etl.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.ConnectionStringName));
                    _writer.WriteString(etl.ConnectionStringName);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Transforms));
                    WriteTransforms(etl.Transforms);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.Disabled));
                    _writer.WriteBool(etl.Disabled);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.AllowEtlOnNonEncryptedChannel));
                    _writer.WriteBool(etl.AllowEtlOnNonEncryptedChannel);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.TestMode));
                    _writer.WriteBool(etl.TestMode);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.LoadRequestTimeoutInSec));
                    if (etl.LoadRequestTimeoutInSec != null)
                        _writer.WriteInteger(etl.LoadRequestTimeoutInSec.Value);
                    else
                        _writer.WriteNull();
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(etl.EtlType));
                    _writer.WriteString(etl.EtlType.ToString());

                    _writer.WriteEndObject();
                }

                _writer.WriteEndArray();
            }

            private void WriteTransforms(List<Transformation> transformation)
            {
                _writer.WriteStartArray();

                var first = true;

                foreach (var transform in transformation)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, transform.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteExternalReplications(List<ExternalReplication> externalReplication)
            {
                if (externalReplication == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var replication in externalReplication)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, replication.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WritePeriodicBackups(List<PeriodicBackupConfiguration> periodicBackup)
            {
                if (periodicBackup == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;

                foreach (var backup in periodicBackup)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    WriteBackupConfiguration(backup);
                }
                _writer.WriteEndArray();
            }

            private void WriteBackupConfiguration(PeriodicBackupConfiguration backup)
            {
                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(backup.TaskId));
                _writer.WriteDouble(backup.TaskId);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.Disabled));
                _writer.WriteBool(backup.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.Name));
                _writer.WriteString(backup.Name);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.MentorNode));
                _writer.WriteString(backup.MentorNode);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.BackupType));
                _writer.WriteString(backup.BackupType.ToString());
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.FullBackupFrequency));
                _writer.WriteString(backup.FullBackupFrequency);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.IncrementalBackupFrequency));
                _writer.WriteString(backup.IncrementalBackupFrequency);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.LocalSettings));
                WriteLocalSettings(backup.LocalSettings);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.S3Settings));
                WriteS3Settings(backup.S3Settings);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.GlacierSettings));
                WriteGlacierSettings(backup.GlacierSettings);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.AzureSettings));
                WriteAzureSettings(backup.AzureSettings);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(backup.FtpSettings));
                WriteFtpSettings(backup.FtpSettings);

                _writer.WriteEndObject();
            }

            private void WriteLocalSettings(LocalSettings localSettings)
            {
                if (localSettings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(localSettings.Disabled));
                _writer.WriteBool(localSettings.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(localSettings.FolderPath));
                _writer.WriteString(localSettings.FolderPath);

                _writer.WriteEndObject();
            }

            private void WriteS3Settings(S3Settings s3Settings)
            {
                if (s3Settings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(s3Settings.Disabled));
                _writer.WriteBool(s3Settings.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(s3Settings.BucketName));
                _writer.WriteString(s3Settings.BucketName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(s3Settings.RemoteFolderName));
                _writer.WriteString(s3Settings.RemoteFolderName);

                _writer.WriteEndObject();
            }

            private void WriteGlacierSettings(GlacierSettings glacierSettings)
            {
                if (glacierSettings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(glacierSettings.Disabled));
                _writer.WriteBool(glacierSettings.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(glacierSettings.VaultName));
                _writer.WriteString(glacierSettings.VaultName);

                _writer.WriteEndObject();
            }

            private void WriteAzureSettings(AzureSettings azureSettings)
            {
                if (azureSettings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(azureSettings.Disabled));
                _writer.WriteBool(azureSettings.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(azureSettings.StorageContainer));
                _writer.WriteString(azureSettings.StorageContainer);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(azureSettings.RemoteFolderName));
                _writer.WriteString(azureSettings.RemoteFolderName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(azureSettings.AccountName));
                _writer.WriteString(azureSettings.AccountName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(azureSettings.AccountKey));
                _writer.WriteString(azureSettings.AccountKey);

                _writer.WriteEndObject();
            }

            private void WriteFtpSettings(FtpSettings ftpSettings)
            {
                if (ftpSettings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(ftpSettings.Url));
                _writer.WriteString(ftpSettings.Url);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.Disabled));
                _writer.WriteBool(ftpSettings.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.Port));
                if (ftpSettings.Port != null)
                    _writer.WriteInteger(ftpSettings.Port.Value);
                else
                    _writer.WriteNull();
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.UserName));
                _writer.WriteString(ftpSettings.UserName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.Password));
                _writer.WriteString(ftpSettings.Password);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.CertificateAsBase64));
                _writer.WriteString(ftpSettings.CertificateAsBase64);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(ftpSettings.CertificateFileName));
                _writer.WriteString(ftpSettings.CertificateFileName);

                _writer.WriteEndObject();
            }

            private void WriteConflictSolver(ConflictSolver conflictSolver)
            {
                if (conflictSolver == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, conflictSolver.ToJson());
            }

            private void WriteClientConfiguration(ClientConfiguration clientConfiguration)
            {
                if (clientConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, clientConfiguration.ToJson());
            }

            private void WriteExpiration(ExpirationConfiguration expiration)
            {
                if (expiration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(expiration.Disabled));
                _writer.WriteBool(expiration.Disabled);

                if (expiration.DeleteFrequencyInSec.HasValue)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(expiration.DeleteFrequencyInSec));
                    _writer.WriteString(expiration.DeleteFrequencyInSec.Value.ToString());
                }

                _writer.WriteEndObject();
            }

            private void WriteRevisions(RevisionsConfiguration revisions)
            {
                if (revisions == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(revisions.Default));
                WriteRevisionsCollectionConfiguration(revisions.Default);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(revisions.Collections));

                if (revisions.Collections == null)
                {
                    _writer.WriteNull();
                }
                else
                {
                    _writer.WriteStartObject();

                    var first = true;
                    foreach (var collection in revisions.Collections)
                    {
                        if (first == false)
                            _writer.WriteComma();
                        first = false;

                        _writer.WritePropertyName(collection.Key);
                        WriteRevisionsCollectionConfiguration(collection.Value);
                    }

                    _writer.WriteEndObject();
                }


                _writer.WriteEndObject();
            }

            private void WriteRavenConnectionStrings(Dictionary<string, RavenConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var ravenConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(nameof(ravenConnectionString.Key));

                    _writer.WriteStartObject();

                    var value = ravenConnectionString.Value;
                    _writer.WritePropertyName(nameof(value.Name));
                    _writer.WriteString(value.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(value.Database));
                    _writer.WriteString(value.Database);
                    _writer.WriteComma();

                    _writer.WriteArray(nameof(value.TopologyDiscoveryUrls), value.TopologyDiscoveryUrls);

                    _writer.WriteEndObject();
                }

                _writer.WriteEndObject();
            }

            private void WriteSqlConnectionStrings(Dictionary<string, SqlConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var sqlConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(nameof(sqlConnectionString.Key));

                    _writer.WriteStartObject();

                    var value = sqlConnectionString.Value;
                    _writer.WritePropertyName(nameof(value.Name));
                    _writer.WriteString(value.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(value.ConnectionString));
                    _writer.WriteString(value.ConnectionString);

                    _writer.WriteEndObject();
                }

                _writer.WriteEndObject();
            }

            private void WriteRevisionsCollectionConfiguration(RevisionsCollectionConfiguration collectionConfiguration)
            {
                if (collectionConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                if (collectionConfiguration.MinimumRevisionsToKeep.HasValue)
                {
                    _writer.WritePropertyName(nameof(collectionConfiguration.MinimumRevisionsToKeep));
                    _writer.WriteInteger(collectionConfiguration.MinimumRevisionsToKeep.Value);
                    _writer.WriteComma();
                }

                if (collectionConfiguration.MinimumRevisionAgeToKeep.HasValue)
                {
                    _writer.WritePropertyName(nameof(collectionConfiguration.MinimumRevisionAgeToKeep));
                    _writer.WriteString(collectionConfiguration.MinimumRevisionAgeToKeep.Value.ToString());
                    _writer.WriteComma();
                }

                _writer.WritePropertyName(nameof(collectionConfiguration.Disabled));
                _writer.WriteBool(collectionConfiguration.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(collectionConfiguration.PurgeOnDelete));
                _writer.WriteBool(collectionConfiguration.PurgeOnDelete);

                _writer.WriteEndObject();
            }

            public void Dispose()
            {
                _writer.WriteEndObject();
            }
        }

        private class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(BlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexType.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                indexDefinition.Persist(_context, Writer);

                Writer.WriteEndObject();
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexDefinition.Type.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                Writer.WriteIndexDefinition(_context, indexDefinition);

                Writer.WriteEndObject();
            }
        }

        private class StreamCounterActions : StreamActionsBase, ICounterActions
        {
            public void WriteCounter(CounterDetail counterDetail)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.DocId));
                Writer.WriteString(counterDetail.DocumentId);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.Name));
                Writer.WriteString(counterDetail.CounterName);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.Value));
                Writer.WriteInteger(counterDetail.TotalValue);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.ChangeVector));
                Writer.WriteString(counterDetail.ChangeVector);

                Writer.WriteEndObject();
            }

            public StreamCounterActions(BlittableJsonTextWriter writer, string propertyName) : base(writer, propertyName)
            {
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly DatabaseSource _source;
            private HashSet<string> _attachmentStreamsAlreadyExported;
            private readonly IMetadataModifier _modifier;

            public StreamDocumentActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, DatabaseSource source, string propertyName, IMetadataModifier modifier = null)
                : base(writer, propertyName)
            {
                _context = context;
                _source = source;
                _modifier = modifier;
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (item.Attachments != null)
                    throw new NotSupportedException();

                var document = item.Document;
                using (document.Data)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                        WriteUniqueAttachmentStreams(document, progress);

                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    document.EnsureMetadata(_modifier);

                    _context.Write(Writer, document.Data);
                }
            }

            public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                _context.Write(Writer, new DynamicJsonValue
                {
                    ["Key"] = tombstone.LowerId,
                    [nameof(Tombstone.Type)] = tombstone.Type.ToString(),
                    [nameof(Tombstone.Collection)] = tombstone.Collection,
                    [nameof(Tombstone.Flags)] = tombstone.Flags.ToString(),
                    [nameof(Tombstone.ChangeVector)] = tombstone.ChangeVector,
                    [nameof(Tombstone.DeletedEtag)] = tombstone.DeletedEtag,
                    [nameof(Tombstone.Etag)] = tombstone.Etag,
                    [nameof(Tombstone.LastModified)] = tombstone.LastModified,
                });
            }

            public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                _context.Write(Writer, new DynamicJsonValue
                {
                    [nameof(DocumentConflict.Id)] = conflict.Id,
                    [nameof(DocumentConflict.Collection)] = conflict.Collection,
                    [nameof(DocumentConflict.Flags)] = conflict.Flags.ToString(),
                    [nameof(DocumentConflict.ChangeVector)] = conflict.ChangeVector,
                    [nameof(DocumentConflict.Etag)] = conflict.Etag,
                    [nameof(DocumentConflict.LastModified)] = conflict.LastModified,
                    [nameof(DocumentConflict.Doc)] = conflict.Doc,
                });
            }

            public void DeleteDocument(string id)
            {
                // no-op
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException();
            }

            private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                    document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                    {
                        progress.Attachments.ErroredCount++;

                        throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                    }

                    progress.Attachments.ReadCount++;

                    if (_attachmentStreamsAlreadyExported.Add(hash))
                    {
                        using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            if (stream == null)
                            {
                                progress.Attachments.ErroredCount++;
                                throw new ArgumentException($"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                            }
                            WriteAttachmentStream(hash, stream, tag);
                        }
                    }
                }
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            private void WriteAttachmentStream(LazyStringValue hash, Stream stream, string tag)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(Constants.Documents.Metadata.Key);
                Writer.WriteStartObject();

                Writer.WritePropertyName(DocumentItem.ExportDocumentType.Key);
                Writer.WriteString(DocumentItem.ExportDocumentType.Attachment);

                Writer.WriteEndObject();
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Hash));
                Writer.WriteString(hash);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Size));
                Writer.WriteInteger(stream.Length);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
                Writer.WriteString(tag);

                Writer.WriteEndObject();

                Writer.WriteStream(stream);
            }

        }

        private class StreamKeyValueActions<T> : StreamActionsBase, IKeyValueActions<T>
        {
            public StreamKeyValueActions(BlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKeyValue(string key, T value)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();
                Writer.WritePropertyName("Key");
                Writer.WriteString(key);
                Writer.WriteComma();
                Writer.WritePropertyName("Value");
                Writer.WriteString(value.ToString());
                Writer.WriteEndObject();
            }
        }

        private abstract class StreamActionsBase : IDisposable
        {
            protected readonly BlittableJsonTextWriter Writer;

            protected bool First { get; set; }

            protected StreamActionsBase(BlittableJsonTextWriter writer, string propertyName)
            {
                Writer = writer;
                First = true;

                Writer.WriteComma();
                Writer.WritePropertyName(propertyName);
                Writer.WriteStartArray();
            }

            public void Dispose()
            {
                Writer.WriteEndArray();
            }
        }
    }
}
