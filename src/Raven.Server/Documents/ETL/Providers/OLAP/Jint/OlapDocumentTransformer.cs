using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    internal partial class OlapDocumentTransformer
    {
        protected override void AddLoadedAttachmentJint(JsValue reference, string name, Attachment attachment)
        {
            throw new NotSupportedException("Attachments aren't supported by OLAP ETL");
        }

        protected override void AddLoadedCounterJint(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by OLAP ETL");
        }

        protected override void AddLoadedTimeSeriesJint(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by OLAP ETL");
        }

        private JsValue LoadToFunctionTranslatorJint(JsValue self, JsValue[] args)
        {
            var methodSignature = "loadTo(name, key, obj)";

            if (args.Length != 3)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 3 parameters");

            if (args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

            if (args[2].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

            return LoadToFunctionTranslatorInternal(args[0].AsString(), args[1].AsObject(), args[2].AsObject(), methodSignature);
        }

        private JsValue LoadToFunctionTranslatorJint(string name, JsValue[] args)
        {
            var methodSignature = $"loadTo{name}(key, obj)";

            if (args.Length != 2)
                ThrowInvalidScriptMethodCall($"{methodSignature} must be called with exactly 2 parameters");

            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

            if (args[0].IsObject() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'key' must be an object");

            return LoadToFunctionTranslatorInternal(name, args[0].AsObject(), args[1].AsObject(), methodSignature);
        }

        private JsValue LoadToFunctionTranslatorInternal(string name, ObjectInstance key, ObjectInstance obj, string methodSignature)
        {
            var objectInstance = key;
            if (objectInstance.HasOwnProperty(PartitionKeys) == false)
                ThrowInvalidScriptMethodCall(
                    $"{methodSignature} argument 'key' must have {PartitionKeys} property. Did you forget to use 'partitionBy(p)' / 'noPartition()' ? ");

            var partitionBy = objectInstance.GetOwnProperty(PartitionKeys).Value;
            var result = new ScriptRunnerResult(DocumentScript, new JsHandle(obj));

            if (partitionBy.IsNull())
            {
                // no partition
                LoadToFunction(name, key: name, result);
                return result.Instance.Jint.Item;
            }

            if (partitionBy.IsArray() == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} property {PartitionKeys} of argument 'key' must be an array instance");

            var sb = new StringBuilder(name);
            var arr = partitionBy.AsArray();
            var partitions = new List<string>((int)arr.Length);

            foreach (var item in arr)
            {
                if (item.IsArray() == false)
                    ThrowInvalidScriptMethodCall($"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances");

                var tuple = item.AsArray();
                if (tuple.Length != 2)
                    ThrowInvalidScriptMethodCall(
                        $"{methodSignature} items in array {PartitionKeys} of argument 'key' must be array instances of size 2, but got '{tuple.Length}'");

                sb.Append('/');
                string val = tuple[1].IsDate() 
                    ? tuple[1].AsDate().ToDateTime().ToString(DateFormat) 
                    : tuple[1].ToString();

                var partition = $"{tuple[0]}={val}";
                sb.Append(partition);
                partitions.Add(partition);
            }

            LoadToFunction(name, sb.ToString(), result, partitions);
            return result.Instance.Jint.Item;
        }

        private JsValue PartitionByJint(JsValue self, JsValue[] args)
        {
            if (args.Length == 0)
                ThrowInvalidScriptMethodCall("partitionBy(args) cannot be called with 0 arguments");

            var engineEx = (JintEngineEx)DocumentEngineHandle;
            JsValue array;
            if (args.Length == 1 && args[0].IsArray() == false)
            {
                array = JsValue.FromObject(engineEx, new[]
                {
                    JsValue.FromObject(engineEx, new[]
                    {
                        new JsString(DefaultPartitionColumnName), args[0]
                    })
                });
            }
            else
            {
                array = JsValue.FromObject(engineEx, args);
            }

            var o = new ObjectInstance(engineEx);
            o.FastAddProperty(PartitionKeys, array, false, true, false);

            return o;
        }

        private JsValue NoPartitionJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("noPartition() must be called with 0 parameters");

            if (_noPartition.IsEmpty)
            {
                var engineEx = (JintEngineEx)DocumentEngineHandle;
                _noPartition = new JsHandle(new ObjectInstance(engineEx));
                _noPartition.Jint.Obj.FastAddProperty(PartitionKeys, JsValue.Null, false, true, false);
            }

            return _noPartition.Jint.Obj;
        }
    }
}
