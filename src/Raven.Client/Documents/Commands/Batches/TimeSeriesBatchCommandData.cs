using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class IncrementalTimeSeriesBatchCommandData : TimeSeriesCommandData
    {
        public IncrementalTimeSeriesBatchCommandData(string documentId, string name, IList<TimeSeriesOperation.IncrementOperation> increments) : base(documentId, name)
        {
            if (increments != null)
            {
                foreach (var incrementOperation in increments)
                {
                    TimeSeries.Increment(incrementOperation);
                }
            }
        }

        public override CommandType Type => CommandType.TimeSeriesWithIncrements;
    }

    public class TimeSeriesBatchCommandData : TimeSeriesCommandData
    {
        public TimeSeriesBatchCommandData(string documentId, string name, IList<TimeSeriesOperation.AppendOperation> appends, 
            List<TimeSeriesOperation.DeleteOperation> deletes) : base(documentId, name)
        {
            if (appends != null)
            {
                foreach (var appendOperation in appends)
                {
                    TimeSeries.Append(appendOperation);
                }
            }

            if (deletes != null)
            {
                foreach (var deleteOperation in deletes)
                {
                    TimeSeries.Delete(deleteOperation);
                }
            }
        }

        public override CommandType Type => CommandType.TimeSeries;
    }

    public abstract class TimeSeriesCommandData : ICommandData
    {
        protected TimeSeriesCommandData(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;

            TimeSeries = new TimeSeriesOperation
            {
                Name = name
            };
        }

        public string Id { get; set; }

        public string Name { get; set; }

        public string ChangeVector => null;

        public abstract CommandType Type { get; }

        public TimeSeriesOperation TimeSeries { get; protected set; }
        
        public bool? FromEtl { get; set; }

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(TimeSeries)] = TimeSeries.ToJson(),
                [nameof(Type)] = Type
            };

            if (FromEtl.HasValue)
                result[nameof(FromEtl)] = FromEtl;
            
            return result;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return ToJson();
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
