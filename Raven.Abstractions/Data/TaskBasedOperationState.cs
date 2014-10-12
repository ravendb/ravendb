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

        public TaskBasedOperationState(Task task)
        {
            this.task = task;
        }

        public bool Completed
        { 
            get
            {
                return task.IsCompleted; 
            }
        }

        public bool Faulted 
        {
            get
            {
                return task.IsFaulted;
            }
        }

        public RavenJToken State { 
            get
            {
                if (!Faulted)
                {
                    return null;
                }
                return RavenJObject.FromObject(new
                                               {
                                                   Error = task.Exception.ExtractSingleInnerException().Message
                                               });
            }
        } 
    }
}