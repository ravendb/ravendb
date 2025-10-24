using System;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch;

internal static class EngineExtensions
{
    /// <summary>
    /// Sets the <paramref name="func"/> under a specific <paramref name="name"/>.
    /// </summary>
    public static void SetFunc(this Engine engine, string name, Func<JsValue, JsValue[], JsValue> func) => engine.SetValue(name, new ClrFunction(engine, name, func));
}
