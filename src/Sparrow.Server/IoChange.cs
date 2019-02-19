using System;
using Sparrow.Server.Meters;

namespace Sparrow.Server
{
    public class IoChange
    {
        public string FileName { get; set; }

        public IoMeterBuffer.MeterItem MeterItem { get; set; }
    }

    public class IoChangesNotifications
    {
        public event Action<IoChange> OnIoChange;

        public void RaiseNotifications(string fileName, IoMeterBuffer.MeterItem meterItem)
        {
            OnIoChange?.Invoke(new IoChange
            {
                FileName = fileName,
                MeterItem = meterItem
            });
        }
    }
}