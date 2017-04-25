using System;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Transformers;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, PutValueCommand> PutValueCommand = GenerateJsonDeserializationRoutine<PutValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteDatabaseCommand> DeleteDatabaseCommand = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();
        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();
        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromDatabaseCommand> RemoveNodeFromDatabaseCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>();
        
        public static Dictionary<string, Func<BlittableJsonReaderObject, UpdateDatabaseCommand>> UpdateDatabaseCommands = new Dictionary<string, Func<BlittableJsonReaderObject, UpdateDatabaseCommand>>()
        {
            [nameof(EditVersioningCommand)] = GenerateJsonDeserializationRoutine<EditVersioningCommand>(),
            
            [nameof(PutTransformerCommand)] = GenerateJsonDeserializationRoutine<PutTransformerCommand>(),
            [nameof(DeleteTransformerCommand)] = GenerateJsonDeserializationRoutine<DeleteTransformerCommand>(),
            [nameof(SetTransformerLockCommand)] = GenerateJsonDeserializationRoutine<SetTransformerLockCommand>(),
            [nameof(RenameTransformerCommand)] = GenerateJsonDeserializationRoutine<RenameTransformerCommand>(),

            [nameof(PutIndexCommand)] = GenerateJsonDeserializationRoutine<PutIndexCommand>(),
            [nameof(PutAutoIndexCommand)] = GenerateJsonDeserializationRoutine<PutAutoIndexCommand>(),
            [nameof(DeleteIndexCommand)] = GenerateJsonDeserializationRoutine<DeleteIndexCommand>(),
            [nameof(SetIndexLockCommand)] = GenerateJsonDeserializationRoutine<SetIndexLockCommand>(),
            [nameof(SetIndexPriorityCommand)] = GenerateJsonDeserializationRoutine<SetIndexPriorityCommand>(),
            [nameof(ModifyDatabaseWatchersCommand)] = GenerateJsonDeserializationRoutine<ModifyDatabaseWatchersCommand>(),
            [nameof(ModifyConflictSolverCommand)] = GenerateJsonDeserializationRoutine<ModifyConflictSolverCommand>(),
        };

        public static readonly Func<BlittableJsonReaderObject, ServerStore.PutRaftCommandResult> PutRaftCommandResult = GenerateJsonDeserializationRoutine<ServerStore.PutRaftCommandResult>();
    }
}