using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.JsonPatch.Operations;
using NCrontab.Advanced.Extensions;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Operation = Microsoft.AspNetCore.JsonPatch.Operations.Operation;

namespace Raven.Server.Documents.TransactionCommands
{
    public class JsonPatchCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly string _id;
        private readonly List<Command> _commands;
        public JsonPatchResult _patchResult;
        protected readonly bool _returnDocument;
        private readonly JsonOperationContext _externalContext;

        public JsonPatchCommand(string id, List<Command> commands, bool returnDocument, JsonOperationContext externalContext)
        {
            _id = id;
            _commands = commands;
            _externalContext = externalContext;
            _returnDocument = returnDocument;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var document = context.DocumentDatabase.DocumentsStorage.Get(context, _id);
            
            if (document == null)
            {
                throw new InvalidOperationException($"Cannot apply json patch because the document {_id} does not exist");
            }

            try
            {
                var isModified = false;
                foreach (Command command in _commands)
                {
                    var pathBlittable = command.GetActionableObject(document, command.Paths, command.OriginalPath);
                    string prop = command.Paths[^1];

                    switch (command.Type)
                    {
                        case CommandTypes.Add:
                            command.Add(pathBlittable, prop, command.Value);
                            break;
                        case CommandTypes.Remove:
                            command.Remove(pathBlittable, prop);
                            break;
                        case CommandTypes.Replace:
                            command.Replace(pathBlittable, prop, command.Value);
                            break;
                        case CommandTypes.Move:
                            command.Move(pathBlittable, prop, command.GetActionableObject(document, command.FromPaths, command.OriginalFrom), command.FromPaths[^1]);
                            break;
                        case CommandTypes.Copy:
                            command.Copy(pathBlittable, prop, command.GetNonActionableObject(document, command.FromPaths, command.OriginalFrom));
                            break;
                        case CommandTypes.Test:
                            command.Test(command.GetNonActionableObject(document, command.Paths, command.OriginalPath), command.Value);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Unsupported command type '{command.Type}'.");
                    }

                    if (command.Type == CommandTypes.Test)
                        continue;

                    using (document.Data)
                    {
                        isModified = true;
                        // ReadObject has to be called after every command so changes between each command are applied for the next operation's use.
                        // For example: executing an Add at /a/b which would result in 'b' being created, then executing a Replace at /a/b
                        // Without ReadObject after every command, TryGet will not find 'b' for the Replace operation.
                        document.Data = context.ReadObject(document.Data, _id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }
                }

                DocumentsStorage.PutOperationResults? putResult = null;
                if (isModified)
                    putResult = context.DocumentDatabase.DocumentsStorage.Put(context, _id, document.ChangeVector, document.Data);

                if (putResult == null)
                {
                    _patchResult = new JsonPatchResult {Status = PatchStatus.NotModified,};
                }
                else
                {
                    _patchResult = new JsonPatchResult
                    {
                        Status = PatchStatus.Patched,
                        ModifiedDocument = document.Data?.Clone(_externalContext),
                        ChangeVector = putResult.Value.ChangeVector,
                        Collection = putResult.Value.Collection.Name,
                        LastModified = putResult.Value.LastModified
                    };
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"An error occurred while trying to apply json patch operation to document {_id}.", ex);
            }

            return 1;
        }

        public string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections, DocumentDatabase database)
        {
            if (_patchResult.ModifiedDocument != null)
                database.HugeDocuments.AddIfDocIsHuge(_id, _patchResult.ModifiedDocument.Size);
            
            if (_patchResult.Collection != null)
                modifiedCollections?.Add(_patchResult.Collection);

            var patchReply = new DynamicJsonValue
            {
                [nameof(BatchRequestParser.CommandData.Id)] = _id,
                [nameof(BatchRequestParser.CommandData.ChangeVector)] = _patchResult.ChangeVector,
                [nameof(Constants.Documents.Metadata.LastModified)] = _patchResult.LastModified,
                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.JsonPatch),
                [nameof(PatchStatus)] = _patchResult.Status
            };

            if (_returnDocument)
                patchReply[nameof(PatchResult.ModifiedDocument)] = _patchResult.ModifiedDocument;

            reply.Add(patchReply);
            
            return _patchResult.ChangeVector;
        }
        
        public record Command(CommandTypes Type, string[] Paths, string[] FromPaths, string OriginalPath, string OriginalFrom, object Value)
        {
            public BlittableJsonReaderBase GetActionableObject(Document document, string[] paths, string fullPath)
            {
                BlittableJsonReaderBase root = document.Data;
                
                for (int i = 1; i < paths.Length - 1; i++)
                {
                    TryGetMember(root, paths[i], out var member, fullPath);
                    
                    if (member is BlittableJsonReaderObject nested)
                        root = nested;
                    else if (member is BlittableJsonReaderArray arr)
                    {
                        if(i == paths.Length - 2)
                            return arr;
                        root = arr;
                    }
                    else
                        throw new ArgumentException($"Cannot reach target location. Failed to fetch '{paths[i+1]}' in path '{fullPath}' for operation '{Type}' because element at {paths[i]} is not an object or a collection");
                }

                return root;
            }

            public object GetNonActionableObject(Document document, string[] paths, string fullPath)
            {
                var cur = GetActionableObject(document, paths, fullPath);
                TryGetMember(cur, paths[^1], out object obj, fullPath);
                return obj;
            }

            private void TryGetMember(BlittableJsonReaderBase objectToSearchIn, string name, out object result, string fullPath)
            {
                switch (objectToSearchIn)
                {
                    case BlittableJsonReaderArray arr:
                        if (int.TryParse(name, out int index) == false)
                            throw new ArgumentException($"Expected an index but got a '{name}' at path {fullPath}");
                        if (index >= arr.Length || index < 0)
                            throw new ArgumentOutOfRangeException($"Position {index} is out of array bounds at array {fullPath}");
                        result = arr[index];
                        break;

                    case BlittableJsonReaderObject blittable:
                        if (blittable.TryGetMember(name, out object obj) == false)
                        {
                            goto default;
                        }
                        result = obj;
                        break;
                    default:
                        throw new ArgumentException($"Cannot reach target location. Failed to fetch '{name}' in path '{fullPath}' for operation '{Type}'");
                }
            }

            public void Test(object objectAtPath, object value)
            {
                //All Raven internal structures implement Equals
                if (objectAtPath.Equals(value) == false)
                {
                    throw new InvalidOperationException($"The current value '{objectAtPath}' is not equal to the test value '{value}'");
                }
            }

            public void Move(BlittableJsonReaderBase targetBlittable, string targetProp, BlittableJsonReaderBase fromBlittable, string fromProp)
            {
                TryGetMember(fromBlittable, fromProp, out object result, OriginalFrom);
                Remove(fromBlittable, fromProp);
                Add(targetBlittable, targetProp, result);
            }

            public void Copy(BlittableJsonReaderBase targetBlittable, string prop, object fromObj)
            {
                Add(targetBlittable, prop, fromObj);
            }

            public void Remove(BlittableJsonReaderBase targetBlittable, string prop)
            {
                TryGetMember(targetBlittable, prop, out object result, OriginalPath); //checks if prop to remove exists

                switch (targetBlittable)
                {
                    case BlittableJsonReaderArray arr:
                        arr.EnsureArrayModifiable();
                        arr.Modifications.Items.RemoveAt(int.Parse(prop)); // Parse is sure to work because of TryGetMember
                        break;

                    case BlittableJsonReaderObject obj:
                        obj.Modifications ??= new DynamicJsonValue(obj);
                        obj.Modifications.Remove(prop);
                        break;

                    default:
                        ThrowUnsupportedType();
                        break;
                }
            }

            public void Add(BlittableJsonReaderBase targetBlittable, string prop, object value)
            {
                switch (targetBlittable)
                {
                    case BlittableJsonReaderArray arr:
                        arr.EnsureArrayModifiable();
                        if (prop == "-")
                        {
                            arr.Modifications.Items.Add(value);
                            return;
                        }
                        if (int.TryParse(prop, out var index) == false)
                            throw new ArgumentException($"Expected an index but got a '{prop}' at path {OriginalPath}");
                        if (index >= arr.Length || index < 0)
                            throw new ArgumentOutOfRangeException($"Position {index} is out of array bounds at array {OriginalPath}");
                        
                        arr.Modifications.Items.Insert(index, value);
                        break;

                    case BlittableJsonReaderObject obj:
                        obj.Modifications ??= new DynamicJsonValue(obj);
                        obj.Modifications[prop] = value;
                        break;
                    default:
                        ThrowUnsupportedType();
                        break;
                }
            }

            public void Replace(BlittableJsonReaderBase blittable, string prop, object value)
            {
                TryGetMember(blittable, prop, out object result, OriginalPath);
                switch (blittable)
                {
                    case BlittableJsonReaderArray arr:
                    {
                        arr.EnsureArrayModifiable();
                        arr.Modifications.Items[int.Parse(prop)] = value; // Parse is sure to work because of TryGetMember
                        break;
                    }
                    case BlittableJsonReaderObject obj:
                        obj.Modifications ??= new DynamicJsonValue(obj);
                        obj.Modifications[prop] = value;
                        break;
                    default:
                        ThrowUnsupportedType();
                        break;
                }
            }

            private void ThrowUnsupportedType()
            {
                throw new NotSupportedException("Unsupported blittable type");
            }
        }

        public enum CommandTypes
        {
            Add,
            Remove,
            Replace,
            Move,
            Copy,
            Test
        }

        public static List<Command> Parse(BlittableJsonReaderObject blittable)
        {
            var commands = new List<Command>();

            blittable.TryGet("Operations", out BlittableJsonReaderArray commandsBlittable);

            foreach (BlittableJsonReaderObject command in commandsBlittable)
            {
                if (command.TryGet(nameof(Operation.op), out string op) == false ||
                    Enum.TryParse(op, ignoreCase: true, out CommandTypes type) == false)
                    throw new ArgumentException($"Unable to understand 'op' {op}");

                if (command.TryGet(nameof(Operation.path), out string path) == false)
                    throw new ArgumentException("Unable to find 'path' property");

                if ((command.TryGet(nameof(Operation.from), out string from) == false || string.IsNullOrWhiteSpace(from)) &&
                    (op == nameof(OperationType.Move).ToLower() ||
                     op == nameof(OperationType.Copy).ToLower()))
                    throw new ArgumentException($"Unable to find 'from' property for operation '{op}'");

                if (command.TryGet(nameof(Operation.value), out object value) == false &&
                    (op == nameof(OperationType.Add).ToLower() ||
                     op == nameof(OperationType.Replace).ToLower() ||
                     op == nameof(OperationType.Test).ToLower()))
                    throw new ArgumentException($"Unable to find 'value' property for operation '{op}'");

                var paths = path.Split('/');
                var fromPaths = from?.Split('/');
                
                for (int i = 1; i < paths.Length; i++)
                {
                    if(paths[i].IsNullOrWhiteSpace())
                        throw new ArgumentException($"Invalid path {path}. Paths must be of format /*/*/*... ");
                    paths[i] = EscapePathMember(paths[i]);
                }

                for (int i = 1; fromPaths != null && i < fromPaths.Length; i++)
                {
                    if (fromPaths[i].IsNullOrWhiteSpace())
                        throw new ArgumentException($"Invalid 'from' path {path}. Paths must be of format /*/*/*... ");
                    fromPaths[i] = EscapePathMember(fromPaths[i]);
                }

                commands.Add(new Command(type, paths, fromPaths, path, from, value));
            }

            return commands;
        }

        private static string EscapePathMember(string member)
        {
            return member.Replace("~0", "~").Replace("~1", "/");
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new JsonPatchCommandDto
            {
                Id = _id,
                Commands = _commands,
                ReturnDocument = _returnDocument,
                ExternalContext = _externalContext
            };
        }

        public class JsonPatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand>
        {
            public string Id;
            public List<Command> Commands;
            public bool ReturnDocument;
            public JsonOperationContext ExternalContext;

            TransactionOperationsMerger.MergedTransactionCommand TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand>.ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var jsonPatchCommand = new JsonPatchCommand(Id, Commands, ReturnDocument, ExternalContext);
                return jsonPatchCommand;
            }
        }
    }
}
