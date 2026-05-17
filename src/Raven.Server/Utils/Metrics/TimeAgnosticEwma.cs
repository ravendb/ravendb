namespace Raven.Server.Utils.Metrics;

public sealed class TimeAgnosticEwma
{
    private const double Alpha = 0.05;
    private double _ewmaErrors;
    private double _ewmaTotalItems;

    private bool _initialized;

    public void Reset()
    {
        _ewmaErrors = 0;
        _ewmaTotalItems = 0;
        _initialized = false;
    }

    public void UpdateOnBatchCompletion(long errorsInThisBatch, long totalItemsInThisBatch)
    {
        if (totalItemsInThisBatch == 0)
            return;
        
        if (_initialized == false)
        {
            _ewmaErrors = errorsInThisBatch;
            _ewmaTotalItems = totalItemsInThisBatch;
            _initialized = true;
        }
        else
        {
            _ewmaErrors = _ewmaErrors * (1 - Alpha) + errorsInThisBatch * Alpha;
            _ewmaTotalItems = _ewmaTotalItems * (1 - Alpha) + totalItemsInThisBatch * Alpha;
        }
    }

    public double GetRate()
    {
        if (_ewmaTotalItems == 0)
            return 0; 
        
        return _ewmaErrors / _ewmaTotalItems;
    }
}
