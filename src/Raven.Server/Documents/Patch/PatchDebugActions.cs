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
        
        public readonly DynamicJsonArray GetTimeSeries = new DynamicJsonArray();
        public readonly DynamicJsonArray AppendTimeSeries = new DynamicJsonArray();
        public readonly DynamicJsonArray IncrementTimeSeries = new DynamicJsonArray();
        public readonly DynamicJsonArray DeleteTimeSeries = new DynamicJsonArray();
     
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
                  
                [nameof(GetTimeSeries)] = new DynamicJsonArray(GetTimeSeries.Items),
                [nameof(AppendTimeSeries)] = new DynamicJsonArray(AppendTimeSeries.Items),
                [nameof(IncrementTimeSeries)] = new DynamicJsonArray(IncrementTimeSeries.Items),
                [nameof(DeleteTimeSeries)] = new DynamicJsonArray(DeleteTimeSeries.Items)
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
            
            GetTimeSeries.Clear();
            AppendTimeSeries.Clear();
            IncrementTimeSeries.Clear();
            DeleteTimeSeries.Clear();
        }
    }
}
