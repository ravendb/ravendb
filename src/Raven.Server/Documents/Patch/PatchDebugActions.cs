using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class PatchDebugActions
    {
        public readonly DynamicJsonArray LoadDocument = new DynamicJsonArray();
        public readonly DynamicJsonArray PutDocument = new DynamicJsonArray();
        public readonly DynamicJsonArray DeleteDocument = new DynamicJsonArray();
       
        public readonly DynamicJsonArray GetCounter = new DynamicJsonArray();
        public readonly DynamicJsonArray IncrementCounter = new DynamicJsonArray();
        public readonly DynamicJsonArray DeleteCounter = new DynamicJsonArray();
        
        public readonly DynamicJsonArray GetTimeseries = new DynamicJsonArray();
        public readonly DynamicJsonArray AppendTimeseries = new DynamicJsonArray();
        public readonly DynamicJsonArray DeleteTimeseries = new DynamicJsonArray();
     
        public DynamicJsonValue GetDebugActions()
        {
            return new DynamicJsonValue
            {
                [nameof(LoadDocument)] = new DynamicJsonArray(LoadDocument.Items),
                [nameof(PutDocument)] = new DynamicJsonArray(PutDocument.Items),
                [nameof(DeleteDocument)] = new DynamicJsonArray(DeleteDocument.Items),

                [nameof(GetCounter)] = new DynamicJsonArray(GetCounter.Items),
                [nameof(IncrementCounter)] = new DynamicJsonArray(IncrementCounter.Items),
                [nameof(DeleteCounter)] = new DynamicJsonArray(DeleteCounter.Items),
                  
                [nameof(GetTimeseries)] = new DynamicJsonArray(GetTimeseries.Items),
                [nameof(AppendTimeseries)] = new DynamicJsonArray(AppendTimeseries.Items),
                [nameof(DeleteTimeseries)] = new DynamicJsonArray(DeleteTimeseries.Items)
            };
        }

        public void Clear()
        {
            LoadDocument.Clear();
            PutDocument.Clear();
            DeleteDocument.Clear();
            
            GetCounter.Clear();
            IncrementCounter.Clear();
            DeleteCounter.Clear();
            
            GetTimeseries.Clear();
            AppendTimeseries.Clear();
            DeleteTimeseries.Clear();
        }
    }
}
