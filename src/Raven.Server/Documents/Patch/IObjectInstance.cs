using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    //public interface IObjectInstance : IDisposable
    //{
    //    IJsEngineHandle<T> EngineHandle { get; } 

    //    JsHandle CreateJsHandle(bool keepAlive = false); // TODO [shlomo] may be eliminated with care
    //}

    public interface IObjectInstance<T> : IDisposable
        where T : struct, IJsHandle<T>
    {
        IJsEngineHandle<T> EngineHandle { get; }

        T CreateJsHandle(bool keepAlive = false); // TODO [shlomo] may be eliminated with care
    }
}
