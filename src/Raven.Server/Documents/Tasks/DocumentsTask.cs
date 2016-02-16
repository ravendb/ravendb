//-----------------------------------------------------------------------
// <copyright file="DocumentsTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

using Raven.Server.Json;

namespace Raven.Server.Documents.Tasks
{
    public abstract class DocumentsTask
    {
        public readonly int IndexId;

        public abstract DocumentsTaskType Type { get; }

        public abstract int NumberOfKeys { get; }

        public abstract void Merge(DocumentsTask task);

        public abstract void Execute();

        protected DocumentsTask(int indexId)
        {
            IndexId = indexId;
        }

        public abstract BlittableJsonReaderObject ToJson(RavenOperationContext context);

        public static DocumentsTask ToTask(BlittableJsonReaderObject json)
        {
            int type;
            if (json.TryGet("Type", out type) == false)
                throw new InvalidOperationException("Invalid JSON");

            switch ((DocumentsTaskType)type)
            {
                case DocumentsTaskType.RemoveFromIndex:
                    return RemoveFromIndexTask.ToTask(json);
                default:
                    throw new NotSupportedException($"Could not convert {type} task");
            }
        }

        public abstract DocumentsTask Clone();

        public enum DocumentsTaskType
        {
            RemoveFromIndex = 0
        }
    }
}
