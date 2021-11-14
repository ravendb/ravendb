using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using V8.Net;


namespace Raven.Server.Documents.Patch.V8
{

    public class TaskCustomBinder : ObjectBinderEx<Task>
    {
        public static InternalHandle CreateObjectBinder(V8EngineEx engine, Task oi, bool keepAlive = false) 
        {
            return engine.CreateObjectBinder<TaskCustomBinder>(oi, engine.TypeBinderTask, keepAlive: keepAlive);
        }

        public static InternalHandle GetRunningTaskResult(V8Engine engine, Task task)
        {
            var value = $"{{Ignoring Task.Result as task's status is {task.Status.ToString()}}}.";
            if (task.IsFaulted)
                value += Environment.NewLine + "Exception: " + task.Exception;
            return engine.CreateValue(value);
        }

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            if (ObjClr.IsCompleted == false && propertyName == nameof(Task<int>.Result))
            {
                return GetRunningTaskResult(Engine, ObjClr);
            }
            return base.NamedPropertyGetter(ref propertyName);
        }

    }
}
