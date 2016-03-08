// -----------------------------------------------------------------------
//  <copyright file="TaskBasedOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Json.Linq;

using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Data
{
    public class TaskBasedOperationState : IOperationState
    {
        private readonly Task task;

        private readonly RavenJToken stateOverride;

        public TaskBasedOperationState(Task task, RavenJToken stateOverride = null)
        {
            this.task = task;
            this.stateOverride = stateOverride;
        }

        public bool Completed => task.IsCompleted;

        public bool Faulted => task.IsFaulted;

        public RavenJToken State
        {
            get
            {
                if (!Faulted)
                {
                    return stateOverride;
                }
                return RavenJObject.FromObject(new
                                               {
                                                   Error = task.Exception.ExtractSingleInnerException().Message
                                               });
            }
        }
    }
}
