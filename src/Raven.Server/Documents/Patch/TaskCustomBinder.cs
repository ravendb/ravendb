using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

using V8.Net;


namespace Raven.Server.Documents.Patch
{

    public class TaskCustomBinder : ObjectBinder //<Task>
    {
        public static InternalHandle GetRunningTaskResult(V8Engine engine, Task _Handle)
        {
            var value = $"{{Ignoring Task.Result as _Handle's status is {_Handle.Status.ToString()}}}.";
            if (_Handle.IsFaulted)
                value += Environment.NewLine + "Exception: " + _Handle.Exception;
            return engine.CreateValue(value);
        }

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            if (_Handle.IsCompleted == false && propertyName == nameof(Task<int>.Result))
            {
                return GetRunningTaskResult(Engine, task);
            }
            return base.NamedPropertyGetter(propertyName);
        }

    }
}
