// -----------------------------------------------------------------------
//  <copyright file="TaskBasedOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Json.Linq;

using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Data
{
    public class TaskBasedOperationState : IOperationState
    {
        private readonly Task task;

        private readonly Func<RavenJObject> stateProvider;

        public TaskBasedOperationState(Task task, Func<RavenJObject> stateProvider = null)
        {
            this.task = task;
            this.stateProvider = stateProvider;
        }

        public Exception Exception
        {
            get
            {
                var ex = (task.IsFaulted || task.IsCanceled) ? task.Exception.ExtractSingleInnerException() : null;

                if (ex == null && (Faulted || Canceled) && State != null) 
                {
                    ex = new Exception(State.Value<string>("Error"));
                }
                return ex;
            }
        }
        public bool Completed => task.IsCompleted;

        public bool Faulted => task.IsFaulted;

        public bool Canceled => task.IsCanceled;

        public RavenJObject State => !Faulted && !Canceled ? stateProvider?.Invoke() : new RavenJObject();
    }
}
