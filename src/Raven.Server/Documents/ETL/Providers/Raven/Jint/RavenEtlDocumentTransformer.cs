using System.Collections.Generic;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;

// ReSharper disable ForCanBeConvertedToForeach

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public partial class RavenEtlDocumentTransformer
    {
        private JsValue AddAttachmentJint(JsValue self, JsValue[] args)
        {
            JsValue attachmentReference = null;
            string name = null; // will preserve original name

            switch (args.Length)
            {
                case 2:
                    if (args[0].IsString() == false)
                        ThrowInvalidScriptMethodCall($"First argument of {Transformation.AddAttachment}(name, attachment) must be string");

                    name = args[0].AsString();
                    attachmentReference = args[1];
                    break;
                case 1:
                    attachmentReference = args[0];
                    break;
                default:
                    ThrowInvalidScriptMethodCall($"{Transformation.AddAttachment} must have one or two arguments");
                    break;
            }

            if (attachmentReference.IsNull())
                return self;

            if (attachmentReference.IsString() == false || attachmentReference.AsString().StartsWith(Transformation.AttachmentMarker) == false)
            {
                var message =
                    $"{Transformation.AddAttachment}() method expects to get the reference to an attachment while it got argument of '{attachmentReference.Type}' type";

                if (attachmentReference.IsString())
                    message += $" (value: '{attachmentReference.AsString()}')";

                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddAttachment(new JsHandle(self), name, new JsHandle(attachmentReference));

            return self;
        }

        private JsValue AddCounterJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Add} must have one arguments");

            var counterReference = args[0];

            if (counterReference.IsNull())
                return self;

            if (counterReference.IsString() == false || counterReference.AsString().StartsWith(Transformation.CountersTransformation.Marker) == false)
            {
                var message =
                    $"{Transformation.CountersTransformation.Add}() method expects to get the reference to a counter while it got argument of '{counterReference.Type}' type";

                if (counterReference.IsString())
                    message += $" (value: '{counterReference.AsString()}')";

                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddCounter(new JsHandle(self), new JsHandle(counterReference));

            return self;
        }
        
        private JsValue AddTimeSeriesJint(JsValue self, JsValue[] args)
        {
            if (args.Length != Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount)
            {
                ThrowInvalidScriptMethodCall(
                    $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} must have {Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount} arguments. " +
                    $"Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`");
            }

            var timeSeriesReference = args[0];

            if (timeSeriesReference.IsNull())
                return self;

            if (timeSeriesReference.IsString() == false || timeSeriesReference.AsString().StartsWith(Transformation.TimeSeriesTransformation.Marker) == false)
            {
                var message =
                    $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} method expects to get the reference to a time-series while it got argument of '{timeSeriesReference.Type}' type";

                if (timeSeriesReference.IsString())
                    message += $" (value: '{timeSeriesReference.AsString()}')";

                message += $". Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`";
                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddTimeSeries(new JsHandle(self), new JsHandle(timeSeriesReference));

            return self;
        }
        
        protected override void AddLoadedAttachmentJint(JsValue reference, string name, Attachment attachment)
        {
            _currentRun.LoadAttachment(new JsHandle(reference), attachment);
        }

        protected override void AddLoadedCounterJint(JsValue reference, string name, long value)
        {
            _currentRun.LoadCounter(new JsHandle(reference), name, value);
        }
        
        protected override void AddLoadedTimeSeriesJint(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            _currentRun.LoadTimeSeries(new JsHandle(reference), name, entries);
        }
   }
}
