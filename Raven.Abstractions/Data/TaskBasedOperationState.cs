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

        private readonly Func<string> stateProvider;

        public TaskBasedOperationState(Task task, Func<string> stateProvider = null)
        {
            this.task = task;
            this.stateProvider = stateProvider;
        }

        public Exception Exception
        {
            get
            {
                var ex = (task.IsFaulted || task.IsCanceled) ? task.Exception.ExtractSingleInnerException() : null;

                if (ex == null && Faulted && State != null)
                {
                    ex = new Exception(State);
                }
                return ex;
            }
        }

        public bool Canceled => task.IsCanceled;

        public bool Completed => task.IsCompleted;

        public bool Faulted => task.IsFaulted;

        public string State
        {
            get { return !Canceled && !Faulted ? stateProvider?.Invoke() : null; }
        }
    }
}
