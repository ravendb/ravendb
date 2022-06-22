using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using JSValueType = V8.Net.JSValueType;

namespace Raven.Server.Documents.Patch;

//public interface IJavaScriptEngineForParsing<T>
//    where T : IJsHandle<T>
//{
//    T GlobalObject { get; }

//    T GetGlobalProperty(string propertyName);

//    void SetGlobalProperty(string name, T value);

//    void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true);

//    void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true);

//    IDisposable DisableConstraints();
//}

//public delegate JsValue JintFunction(JsValue self, JsValue[] args); // TODO [shlomo] to discuss with Pawel moving and using it inside Jint
public interface IJsEngineHandle<T> : /*IJavaScriptEngineForParsing<T>,*/ IScriptEngineChanges, IDisposable
    where T : IJsHandle<T>
{
    T GlobalObject { get; }

    T GetGlobalProperty(string propertyName);

    void SetGlobalProperty(string name, T value);

   

    IDisposable DisableConstraints();
    JavaScriptEngineType EngineType { get; }

    [CanBeNull]
    IJavaScriptOptions JsOptions { get; }

    bool IsMemoryChecksOn { get; }
    T Empty { get; set; }
    T Null { get; set; }
    T Undefined { get; set; }
    T True { get; set; }
    T False { get; set; }
    T ImplicitNull { get; set; }
    T ExplicitNull { get; set; }

    T JsonStringify();

    void ForceGarbageCollection();

    object MakeSnapshot(string name);

    bool RemoveMemorySnapshot(string name);

    void AddToLastMemorySnapshotBefore(T h);

    void RemoveFromLastMemorySnapshotBefore(T h);

    void CheckForMemoryLeaks(string name, bool shouldRemove = true);

    void ResetCallStack();

    void ResetConstraints();

    T FromObjectGen(object obj, bool keepAlive = false);

    T CreateClrCallBack(string propertyName, Func<T, T[], T> func, bool keepAlive = true);

    //    void SetGlobalClrCallBack(string propertyName, (Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple);
    void SetGlobalClrCallBack(string propertyName, Func<T, T[], T> funcTuple);

    T CreateObject();

    T CreateEmptyArray();

   // T CreateArray(System.Array items);

    T CreateArray(IEnumerable<object> items);
    T CreateArray(IEnumerable<T> items);

    T CreateUndefinedValue();

    T CreateNullValue();

    T CreateValue(bool value);

    T CreateValue(Int32 value);

    T CreateValue(double value);
    T CreateValue(long value);

    T CreateValue(string value);

    T CreateValue(TimeSpan ms);

    T CreateValue(DateTime value);

    T CreateError(string message, JSValueType errorType);
}

public interface IScriptEngineChanges
{
    IDisposable ChangeMaxStatements(int value);

    IDisposable ChangeMaxDuration(int value);

    void TryCompileScript(string script);
    
    void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true);

    void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true);
}
