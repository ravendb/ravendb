using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Counters
{
    /// <summary>
    /// Represents a batch of counter operations on multiple documents, supporting retrieval of counter values across nodes.
    /// </summary>
    public sealed class CounterBatch
    {
        /// <summary>
        /// A value indicating whether the response should include counter values from all nodes.
        /// When set to <c>true</c>, the response includes the values of each counter from all nodes in the cluster.
        /// </summary>
        public bool ReplyWithAllNodesValues;

        /// <summary>
        /// Gets or sets the list of counter operations to be performed on the specified documents.
        /// Each <see cref="DocumentCountersOperation"/> represents a set of counter operations for a single document.
        /// </summary>
        public List<DocumentCountersOperation> Documents = new List<DocumentCountersOperation>();

        /// <summary>
        /// Gets or sets a value indicating whether the batch originated from an ETL process.
        /// This is used internally to identify and manage counter operations triggered by ETL pipelines.
        /// </summary>
        public bool FromEtl;
    }

    /// <summary>
    /// Represents counter operations for a specific document, such as increment or set operations on named counters.
    /// </summary>
    public sealed class DocumentCountersOperation
    {
        /// <summary>
        /// A list of counter operations to be performed on the specified document.
        /// Each operation in the list specifies an action, such as incrementing or deleting a counter.
        /// </summary>
        /// <remarks>
        /// The <see cref="Operations"/> field is mandatory and must be populated before the batch operation is executed.
        /// If it is not set or is empty, an exception will be thrown during parsing or execution.
        /// </remarks>
        public List<CounterOperation> Operations;

        /// <summary>
        /// The ID of the document on which the counter operations are to be performed.
        /// </summary>
        /// <remarks>
        /// The <see cref="DocumentId"/> field is mandatory and identifies the target document for the counter operations.
        /// If it is not set, an exception will be thrown during parsing or execution.
        /// </remarks>
        public string DocumentId;

        public static DocumentCountersOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(DocumentId), out string docId) == false || docId == null)
                ThrowMissingDocumentId();

            if (input.TryGet(nameof(Operations), out BlittableJsonReaderArray operations) == false || operations == null)
                ThrowMissingCounterOperations();

            var result = new DocumentCountersOperation
            {
                DocumentId = docId,
                Operations = new List<CounterOperation>()
            };

            foreach (var op in operations)
            {
                if (!(op is BlittableJsonReaderObject bjro))
                {
                    ThrowNotBlittableJsonReaderObjectOperation(op);
                    return null; //never hit
                }

                result.Operations.Add(CounterOperation.Parse(bjro));
            }

            return result;
        }

        private static void ThrowNotBlittableJsonReaderObjectOperation(object op)
        {
            throw new InvalidDataException($"'Operations' should contain items of type BlittableJsonReaderObject only, but got {op.GetType()}");
        }

        private static void ThrowMissingCounterOperations()
        {
            throw new InvalidDataException("Missing 'Operations' property on 'Counters'");
        }

        private static void ThrowMissingDocumentId()
        {
            throw new InvalidDataException("Missing 'DocumentId' property on 'Counters'");
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Operations)] = Operations?.Select(x => x.ToJson())
            };
        }
    }

    /// <summary>
    /// Represents the types of operations that can be performed on counters in RavenDB.
    /// </summary>
    public enum CounterOperationType
    {
        /// <summary>
        /// No operation. Represents the absence of any counter operation.
        /// </summary>
        None,

        /// <summary>
        /// Increases the value of the counter by a specified amount. If the counter does not exist, it will be created with the specified value.
        /// </summary>
        Increment,

        /// <summary>
        /// Deletes the counter, removing it from the database.
        /// </summary>
        Delete,

        /// <summary>
        /// Retrieves the value of a specific counter.
        /// </summary>
        Get,

        /// <summary>
        /// Used internally for sending counters by ETL
        /// </summary>
        Put,

        /// <summary>
        /// Retrieves all counters associated with a document. When using this type, 'CounterName' should not be specified.
        /// </summary>
        GetAll
    }

    /// <summary>
    /// Represents a single counter operation, which defines actions such as incrementing a counter value, deleting a counter or retrieving a counter value for a specific document and counter name.
    /// </summary>
    public sealed class CounterOperation
    {
        /// <summary>
        /// The type of the counter operation to be performed.
        /// Specifies the action, such as incrementing, deleting, or retrieving a counter value.
        /// </summary>
        /// <remarks>
        /// Valid types are defined in the <see cref="CounterOperationType"/> enumeration, including:
        /// - <see cref="CounterOperationType.Increment"/>: Increments the counter by a specified value.
        /// - <see cref="CounterOperationType.Delete"/>: Deletes the counter.
        /// - <see cref="CounterOperationType.Get"/>: Retrieves the value of the counter.
        /// - <see cref="CounterOperationType.GetAll"/>: Retrieves all counters for a document.
        /// </remarks>
        public CounterOperationType Type;

        /// <summary>
        /// The name of the counter to be operated on.
        /// </summary>
        /// <remarks>
        /// - The <see cref="CounterName"/> field is mandatory for operations of type <see cref="CounterOperationType.Increment"/>, <see cref="CounterOperationType.Delete"/>, and <see cref="CounterOperationType.Get"/>.
        /// - For <see cref="CounterOperationType.GetAll"/>, the <see cref="CounterName"/> field is not used and can be <c>null</c>.
        /// - If <see cref="CounterName"/> is required but not set, an exception will be thrown during parsing or execution.
        /// </remarks>
        public string CounterName;

        /// <summary>
        /// The value by which the counter should be incremented or decremented.
        /// </summary>
        /// <remarks>
        /// - Used only for operations of type <see cref="CounterOperationType.Increment"/>.
        /// - For <see cref="CounterOperationType.Put"/>, this specifies the exact value to set for the counter but is used internally and not intended for external use.
        /// - For other operation types, this field is ignored.
        /// </remarks>
        public long Delta = 1;

        internal string ChangeVector;
        internal string DocumentId;

        public static CounterOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(CounterName), out string name) == false || name == null)
                ThrowMissingCounterName();

            if (input.TryGet(nameof(Type), out string type) == false || type == null)
                ThrowMissingCounterOperationType(name);

            var counterOperationType = (CounterOperationType)Enum.Parse(typeof(CounterOperationType), type);

            long? delta = null;
            switch (counterOperationType)
            {
                case CounterOperationType.Increment:
                case CounterOperationType.Put:
                    if (input.TryGet(nameof(Delta), out delta) == false)
                        ThrowMissingDeltaProperty(name, counterOperationType);
                    break;
            }

            var counterOperation = new CounterOperation
            {
                Type = counterOperationType,
                CounterName = name
            };

            if (delta != null)
                counterOperation.Delta = delta.Value;

            return counterOperation;
        }

        private static void ThrowMissingDeltaProperty(string name, CounterOperationType type)
        {
            throw new InvalidDataException($"Missing '{nameof(Delta)}' property in Counter '{name}' of Type {type} ");
        }

        private static void ThrowMissingCounterOperationType(string name)
        {
            throw new InvalidDataException($"Missing '{nameof(Type)}' property in Counter '{name}'");
        }

        private static void ThrowMissingCounterName()
        {
            throw new InvalidDataException($"Missing '{nameof(CounterName)}' property");
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type.ToString(),
                [nameof(CounterName)] = CounterName,
                [nameof(Delta)] = Delta
            };
        }
    }
}
