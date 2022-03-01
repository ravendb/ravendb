using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Patch;
using V8.Net;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Extensions.V8;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    internal partial class OlapDocumentTransformer
    {
        protected override void AddLoadedAttachmentV8(InternalHandle reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by OLAP ETL");
        }

        protected override void AddLoadedCounterV8(InternalHandle reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by OLAP ETL");
        }

        protected override void AddLoadedTimeSeriesV8(InternalHandle reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by OLAP ETL");
        }

        private InternalHandle LoadToFunctionTranslatorV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                var methodSignature = "loadTo(name, key, obj)";

                if (args.Length != 3)
                    ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 3 parameters");

                if (args[0].IsStringEx == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

                if (args[1].IsObject == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

                if (args[2].IsObject == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

                return LoadToFunctionTranslatorInternal(engine, args[0].AsString, args[1], args[2], methodSignature);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadToFunctionTranslatorV8(V8Engine engine, string name, InternalHandle[] args)
        {
            var methodSignature = $"loadTo{name}(key, obj)";

            if (args.Length != 2)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 2 parameters");

            if (args[1].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

            if (args[0].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'key' must be an object");

            return LoadToFunctionTranslatorInternal(engine, name, args[0], args[1], methodSignature);
        }

        private InternalHandle LoadToFunctionTranslatorInternal(V8Engine engine, string name, InternalHandle key, InternalHandle obj, string methodSignature)
        {
            if (key.HasOwnProperty(PartitionKeys) == false)
                ThrowInvalidScriptMethodCall(
                    $"{methodSignature} argument 'key' must have {PartitionKeys} property. Did you forget to use 'partitionBy(p)' / 'noPartition()' ? ");

            using (var partitionBy = key.GetOwnProperty(PartitionKeys))
            {
                var result = new ScriptRunnerResult(DocumentScript, new JsHandle(obj));

                if (partitionBy.IsNull)
                {
                    // no partition
                    LoadToFunction(name, key: name, result);
                    return new InternalHandle(ref result.Instance.V8.Item, true);
                }

                if (partitionBy.IsArray == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} property {PartitionKeys} of argument 'key' must be an array instance");

                var sb = new StringBuilder(name);
                int arrayLength =  partitionBy.ArrayLength;
                var partitions = new List<string>(arrayLength);
                for (int i = 0; i < arrayLength; i++)
                {
                    using (var item = partitionBy.GetProperty(i))
                    {
                        if (item.IsArray == false)
                            ThrowInvalidScriptMethodCall($"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances");

                        if (item.ArrayLength != 2)
                            ThrowInvalidScriptMethodCall(
                                $"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances of size 2, but got '{item.ArrayLength}'");

                        sb.Append('/');
                        using (var tuple1 = item.GetProperty(1))
                        {
                            string val = tuple1.IsDate
                                ? tuple1.AsDate.ToString(DateFormat) 
                                : tuple1.ToString();
                            using (var tuple0 = item.GetProperty(0))
                            {
                                var partition = $"{tuple0}={val}";
                                sb.Append(partition);
                                partitions.Add(partition);
                            }
                        }
                    }
                }
                LoadToFunction(name, sb.ToString(), result, partitions);
                return new InternalHandle(ref result.Instance.V8.Item, true);
            }
        }

        private static InternalHandle PartitionByV8(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args)
        {
            try
            {            
                if (args.Length == 0)
                    ThrowInvalidScriptMethodCall("partitionBy(args) cannot be called with 0 arguments");

                InternalHandle jsArr;
                if (args.Length == 1 && args[0].IsArray == false)
                {
                    jsArr = engine.CreateArray(new[]
                    {
                        engine.CreateArray(new[]
                        {
                            engine.CreateValue(DefaultPartitionColumnName), args[0]
                        })
                    });
                }
                else
                {
                    var engineEx = (V8EngineEx)engine;
                    jsArr = engineEx.FromObject(args);
                }

                InternalHandle o;
                using (jsArr)
                {  
                    o = engine.CreateObject();
                    o.FastAddProperty(PartitionKeys, jsArr, false, true, false);
                }

                return o;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle NoPartitionV8(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args)
        {
            try
            {
                if (args.Length != 0)
                    ThrowInvalidScriptMethodCall("noPartition() must be called with 0 parameters");

                if (_noPartition.IsEmpty)
                {
                    var engineEx = (V8EngineEx)engine;
                    _noPartition = engineEx.CreateObject();
                    _noPartition.V8.Item.FastAddProperty(PartitionKeys, engine.CreateNullValue(), false, true, false);
                }

                return _noPartition.V8.Item.Clone();
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }
    }
}
