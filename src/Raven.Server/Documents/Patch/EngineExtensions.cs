using System;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch;

internal static class EngineExtensions
{
    /// <summary>
    /// Sets the <paramref name="func"/> under a specific <paramref name="name"/>.
    /// </summary>
    public static void SetClrFunc(this Engine engine, string name, Func<JsValue, JsValue[], JsValue> func) => engine.SetValue(name, new ClrFunction(engine, name, func));

    /// <summary>
    /// Sets the given <paramref name="func"/> as a readonly property of <paramref name="obj"/>.
    /// </summary>
    public static void SetClfFunc(this ObjectInstance obj, string name, Func<JsValue, JsValue[], JsValue> func)
    {
        obj.FastSetProperty(name, new PropertyDescriptor(new ClrFunction(obj.Engine, name, func), false, false, false));
    }
}
