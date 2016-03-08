// -----------------------------------------------------------------------
//  <copyright file="IOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class OperationStateBase : IOperationState
    {
        public bool Completed { get; private set; } 
        public bool Faulted { get; private set; }
        public bool Canceled { get; private set; }
        public RavenJObject State { get; protected set; }
        public Exception Exception { get; set; }

        public OperationStateBase()
        {
            State = new RavenJObject();
        }

        public virtual void MarkProgress(string progress)
        {
            State["Progress"] = progress;
        }

        public void MarkCompleted(string state = null)
        {
            VerifyState();
            MarkProgress(state);
            Completed = true;
        }

        public void MarkFaulted(string error = null, Exception exception = null)
        {
            VerifyState();
            MarkProgress(null);
            State["Error"] = error;
            Exception = exception;
            Completed = true;
            Faulted = true;
        }

        public void MarkCanceled(string error = null)
        {
            VerifyState();
            MarkProgress(null);
            State["Error"] = error;
            Completed = true;
            Canceled = true;
        }

        private void VerifyState()
        {
            if (Completed)
            {
                throw new InvalidOperationException("Operation was already marked");
            }
        }
    }
}
