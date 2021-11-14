using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;

// ReSharper disable ForCanBeConvertedToForeach

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public partial class RavenEtlDocumentTransformer
    {
        private InternalHandle AddAttachmentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                InternalHandle jsRes = InternalHandle.Empty;
                InternalHandle attachmentReference = InternalHandle.Empty;
                string name = null; // will preserve original name

                switch (args.Length)
                {
                    case 2:
                        if (args[0].IsStringEx == false)
                            ThrowInvalidScriptMethodCall($"First argument of {Transformation.AddAttachment}(name, attachment) must be string");

                        name = args[0].AsString;
                        attachmentReference = args[1];
                        break;
                    case 1:
                        attachmentReference = args[0];
                        break;
                    default:
                        ThrowInvalidScriptMethodCall($"{Transformation.AddAttachment} must have one or two arguments");
                        break;
                }

                if (attachmentReference.IsNull)
                {
                    return jsRes.Set(self);
                }

                if (attachmentReference.IsStringEx == false || attachmentReference.AsString.StartsWith(Transformation.AttachmentMarker) == false)
                {
                    var message =
                        $"{Transformation.AddAttachment}() method expects to get the reference to an attachment while it got argument of '{attachmentReference.ValueType}' type";

                    if (attachmentReference.IsStringEx)
                        message += $" (value: '{attachmentReference.AsString}')";

                    ThrowInvalidScriptMethodCall(message);
                }

                _currentRun.AddAttachment(new JsHandle(self), name, new JsHandle(attachmentReference));
                return jsRes.Set(self);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle AddCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != 1)
                    ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Add} must have one arguments");

                InternalHandle jsRes = InternalHandle.Empty;
                var counterReference = args[0];

                if (counterReference.IsNull)
                    return jsRes.Set(self);

                if (counterReference.IsStringEx == false || counterReference.AsString.StartsWith(Transformation.CountersTransformation.Marker) == false)
                {
                    var message =
                        $"{Transformation.CountersTransformation.Add}() method expects to get the reference to a counter while it got argument of '{counterReference.ValueType}' type";

                    if (counterReference.IsStringEx)
                        message += $" (value: '{counterReference.AsString}')";

                    ThrowInvalidScriptMethodCall(message);
                }

                _currentRun.AddCounter(new JsHandle(self), new JsHandle(counterReference));

                return jsRes.Set(self);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }
        
        private InternalHandle AddTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
            {
                if (args.Length != Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount)
                {
                    ThrowInvalidScriptMethodCall(
                        $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} must have {Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount} arguments. " +
                        $"Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`");
                }

                InternalHandle jsRes = InternalHandle.Empty;
                var timeSeriesReference = args[0];

                if (timeSeriesReference.IsNull)
                    return jsRes.Set(self);

                if (timeSeriesReference.IsStringEx == false || timeSeriesReference.AsString.StartsWith(Transformation.TimeSeriesTransformation.Marker) == false)
                {
                    var message =
                        $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} method expects to get the reference to a time-series while it got argument of '{timeSeriesReference.ValueType}' type";

                    if (timeSeriesReference.IsStringEx)
                        message += $" (value: '{timeSeriesReference.AsString}')";

                    message += $". Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`";
                    ThrowInvalidScriptMethodCall(message);
                }

                _currentRun.AddTimeSeries(new JsHandle(self), new JsHandle(timeSeriesReference));

                return jsRes.Set(self);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }
        
        protected override void AddLoadedAttachmentV8(InternalHandle reference, string name, Attachment attachment)
        {
            _currentRun.LoadAttachment(new JsHandle(reference), attachment);
        }

        protected override void AddLoadedCounterV8(InternalHandle reference, string name, long value)
        {
            _currentRun.LoadCounter(new JsHandle(reference), name, value);
        }
        
        protected override void AddLoadedTimeSeriesV8(InternalHandle reference, string name, IEnumerable<SingleResult> entries)
        {
            _currentRun.LoadTimeSeries(new JsHandle(reference), name, entries);
        }
   }
}
