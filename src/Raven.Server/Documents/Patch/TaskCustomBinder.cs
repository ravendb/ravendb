﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using V8.Net;


namespace Raven.Server.Documents.Patch
{

    public class TaskCustomBinder : ObjectBinderEx<Task>
    {
        public static InternalHandle GetRunningTaskResult(V8Engine engine, Task task)
        {
            var value = $"{{Ignoring Task.Result as task's status is {task.Status.ToString()}}}.";
            if (task.IsFaulted)
                value += Environment.NewLine + "Exception: " + task.Exception;
            return engine.CreateValue(value);
        }

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            if (objCLR.IsCompleted == false && propertyName == nameof(Task<int>.Result))
            {
                return GetRunningTaskResult(Engine, objCLR);
            }
            return base.NamedPropertyGetter(ref propertyName);
        }

    }
}
