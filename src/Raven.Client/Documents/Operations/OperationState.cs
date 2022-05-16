// -----------------------------------------------------------------------
//  <copyright file="OperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class OperationState
    {
        public IOperationResult Result { get; set; }

        public IOperationProgress Progress { get; set; }

        public OperationStatus Status { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Progress)] = Progress?.ToJson(),
                [nameof(Result)] = Result?.ToJson(),
                [nameof(Status)] = Status.ToString()
            };
        }
    }

    public interface IOperationResult
    {
        string Message { get; }
        DynamicJsonValue ToJson();
        bool ShouldPersist { get; }
        bool CanMerge { get; }
        void MergeWith(IOperationResult result);
    }

    public interface IOperationDetailedDescription
    {
        DynamicJsonValue ToJson();
    }

    public enum OperationStatus
    {
        InProgress,
        Completed,
        Faulted,
        Canceled
    }
}
