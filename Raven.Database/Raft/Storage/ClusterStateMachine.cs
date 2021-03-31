// -----------------------------------------------------------------------
//  <copyright file="ClusterStateMachine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using NetTopologySuite.IO;
using Rachis;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Storage.Handlers;
using Raven.Database.Server.Tenancy;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage
{
    public class ClusterStateMachine : IRaftStateMachine
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly PutSerialLock locker;

        private readonly DocumentDatabase database;

        private readonly Dictionary<Type, CommandHandler> handlers = new Dictionary<Type, CommandHandler>();

        private long lastAppliedIndex;

        internal RaftEngine RaftEngine { get; set; }

        public ClusterStateMachine(DocumentDatabase systemDatabase, DatabasesLandlord databasesLandlord)
        {
            if (systemDatabase == null)
                throw new ArgumentNullException("systemDatabase");

            locker = new PutSerialLock(systemDatabase.Configuration);

            DatabaseHelper.AssertSystemDatabase(systemDatabase);

            database = systemDatabase;

            LastAppliedIndex = ReadLastAppliedIndex();

            handlers.Add(typeof(ClusterConfigurationUpdateCommand), new ClusterConfigurationUpdateCommandHandler(systemDatabase, databasesLandlord));
            handlers.Add(typeof(DatabaseDeletedCommand), new DatabaseDeletedCommandHandler(systemDatabase, databasesLandlord));
            handlers.Add(typeof(DatabaseUpdateCommand), new DatabaseUpdateCommandHandler(systemDatabase, databasesLandlord));
            handlers.Add(typeof(ReplicationStateCommand), new ReplicationStateCommandHandler(systemDatabase, databasesLandlord));
        }

        private long ReadLastAppliedIndex()
        {
            long result = 0;
            database.TransactionalStorage.Batch(accessor =>
            {
                var item = accessor.Lists.Read("Raven/Cluster", "Status");
                if (item == null)
                    return;

                result = item.Data["LastAppliedIndex"].Value<long>();
            });

            return result;
        }

        public void Dispose()
        {
        }

        public long LastAppliedIndex
        {
            get
            {
                return lastAppliedIndex;
            }

            private set
            {
                Thread.VolatileWrite(ref lastAppliedIndex, value);
            }
        }

        public void Apply(LogEntry entry, Command cmd)
        {
            try
            {
                using (locker.Lock())
                {
                    database.TransactionalStorage.Batch(accessor =>
                    {
                        CommandHandler handler;
                        if (handlers.TryGetValue(cmd.GetType(), out handler))
                            handler.Handle(cmd);

                        UpdateLastAppliedIndex(cmd.AssignedIndex, accessor);
                    });
                }
            }
            catch (Exception e)
            {
                log.ErrorException(string.Format("Could not apply command. Type: {0}. Index: {1}.", cmd.GetType(), cmd.AssignedIndex), e);
                throw;
            }
        }

        public bool SupportSnapshots
        {
            get
            {
                return database.Configuration.RunInMemory == false;
            }
        }

        public void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications)
        {
            //TODO: consider move this code to separate class - as RAFT usages will grow we might endup with lots of documents to export
            var directoryPath = Path.Combine(database.Configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory, "Raft", "Snapshot");
            if (Directory.Exists(directoryPath) == false)
                Directory.CreateDirectory(directoryPath);
            var filePath = Path.Combine(directoryPath, string.Format("Full-{0:D19}-{1:D19}.Snapshot", index, term));

            using (var file = new FileStream(filePath, FileMode.Create))
            using (var streamWriter = new StreamWriter(file))
            using (var jsonTextWriter = new JsonTextWriter(streamWriter))
            {

                database.TransactionalStorage.Batch(accessor =>
                {
                    allowFurtherModifications.Set();

                    jsonTextWriter.WriteStartObject();

                    WriteClusterDocumentToSnapshot(jsonTextWriter, accessor, Constants.Cluster.ClusterConfigurationDocumentKey);

                    WriteClusterDocumentToSnapshot(jsonTextWriter, accessor, Constants.Cluster.ClusterReplicationStateDocumentKey);

                    WriteDatabaseDocumentsToSnapshot(accessor, jsonTextWriter);

                    jsonTextWriter.WriteEndObject();

                });

            }
        }

        private static void WriteDatabaseDocumentsToSnapshot(IStorageActionsAccessor accessor, JsonTextWriter jsonTextWriter)
        {
            var databaseDocuments = accessor.Documents.GetDocumentsWithIdStartingWith(Constants.Database.Prefix, 0, int.MaxValue, null);
            foreach (var dbDoc in databaseDocuments)
            {
                if (dbDoc == null)
                {
                    continue;
                }
                jsonTextWriter.WritePropertyName(dbDoc.Key);
                dbDoc.ToJson().WriteTo(jsonTextWriter);
            }
        }

        private static void WriteClusterDocumentToSnapshot(JsonTextWriter jsonTextWriter, IStorageActionsAccessor accessor, string docName)
        {

            jsonTextWriter.WritePropertyName(docName);

            var doc = accessor.Documents.DocumentByKey(docName);
            if (doc != null)
            {
                var json = doc.ToJson();
                json.WriteTo(jsonTextWriter);
            }
            else
            {
                jsonTextWriter.WriteNull();
            }

        }

        public ISnapshotWriter GetSnapshotWriter()
        {
            return new SnapshotWriter(database.Configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory);
        }

        public class SnapshotWriter : ISnapshotWriter
        {
            private readonly string fileName;

            public SnapshotWriter(string dataDirectory)
            {
                var directoryPath = Path.Combine(dataDirectory, "Raft", "Snapshot");
                fileName = Directory.GetFiles(directoryPath, "*.Snapshot").LastOrDefault();

                if (fileName == null)
                    throw new InvalidOperationException("Could not find a full backup file to start the snapshot writing");

                var last = Path.GetFileNameWithoutExtension(fileName);
                Debug.Assert(last != null);
                var parts = last.Split('-');
                if (parts.Length != 3)
                    throw new InvalidOperationException("Invalid snapshot file name " + fileName + ", could not figure out index & term");

                Index = long.Parse(parts[1]);
                Term = long.Parse(parts[2]);
            }

            public long Index { get; private set; }
            public long Term { get; private set; }
            public void WriteSnapshot(Stream stream)
            {
                using (var f = File.OpenRead(fileName))
                {
                    var writer = new BinaryWriter(stream);
                    writer.Write(f.Length);
                    writer.Flush();
                    f.CopyTo(stream);
                }
            }
        }

        public void ApplySnapshot(long term, long index, Stream stream)
        {
            var reader = new BinaryReader(stream);

            var len = reader.ReadInt64();
            var buffer = new byte[16 * 1024];
            var fileBuffer = new byte[len];
            var memoryStream = new MemoryStream(fileBuffer);

            var totalFileRead = 0;
            while (totalFileRead < len)
            {
                var read = stream.Read(buffer, 0, (int)Math.Min(buffer.Length, len - totalFileRead));
                if (read == 0)
                    throw new EndOfStreamException();
                totalFileRead += read;
                memoryStream.Write(buffer, 0, read);
            }

            memoryStream.Position = 0;

            database.TransactionalStorage.Batch(accessor =>
            {
                using (var streamReader = new StreamReader(memoryStream))
                using (var jsonReader = new JsonTextReader(streamReader))
                {
                    if (jsonReader.Read() == false)
                        throw new InvalidDataException("StartObject was expected");
                    if (jsonReader.TokenType != JsonToken.StartObject)
                        throw new InvalidDataException("StartObject was expected");

                    while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
                    {
                        if (jsonReader.TokenType != JsonToken.PropertyName)
                            throw new InvalidDataException("PropertyName was expected");
                        var documentKey = jsonReader.Value.ToString();
                        if (jsonReader.Read() == false)
                            throw new InvalidDataException("StartObject was expected");
                        //The document was missing during the snapshot taking process, this is fine
                        //for all cases but the missing cluster configurations
                        if (jsonReader.TokenType == JsonToken.Null && documentKey != Constants.Cluster.ClusterConfigurationDocumentKey)
                            continue;
                        if (jsonReader.TokenType != JsonToken.StartObject)
                            throw new InvalidDataException("StartObject was expected");
                        var json = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
                        var metadata = json.Value<RavenJObject>(Constants.Metadata) ?? new RavenJObject();
                        json.Remove(Constants.Metadata);
                        accessor.Documents.InsertDocument(documentKey, json, metadata, true);
                    }
                }
                UpdateLastAppliedIndex(index, accessor);
                LastAppliedIndex = index;
            });

        }

        public void Danger__SetLastApplied(long postion)
        {
            LastAppliedIndex = postion;
        }

        private void UpdateLastAppliedIndex(long index, IStorageActionsAccessor accessor)
        {
            accessor.Lists.Set("Raven/Cluster", "Status", new RavenJObject
                                                          {
                                                              { "LastAppliedIndex", index }
                                                          }, UuidType.DocumentReferences);
            accessor.AfterStorageCommit += () => LastAppliedIndex = index;
        }
    }
}