using System;
using System.IO;

namespace Sparrow.Logging
{
    public interface ILogEntry
    {
        DateTime At { set; get; }
        LogMode Type { set; get; }
        string Source { set; get; }
        string Logger { set; get; }
        string Message { set; get; }
        Exception Exception { set; get; }

        public void WriteMessage(StreamWriter writer);
    }
    
    public struct LogEntry : ILogEntry
    {
        public DateTime At { set; get; }
        public LogMode Type { set; get; }
        public string Source { set; get; }
        public string Logger { set; get; }
        public string Message { set; get; }
        public Exception Exception { set; get; }
        public void WriteMessage(StreamWriter writer)
        {
            writer.Write(Message);
        }
    }
    
    public struct LogEntryWithMessageFactory<T> : ILogEntry
    {
        public DateTime At { get; set; }
        public LogMode Type { get; set; }
        public string Source { get; set; }
        public string Logger { get; set; }
        public string Message { get; set; }
        public Exception Exception { get; set; }
        public void WriteMessage(StreamWriter writer)
        {
            writer.Flush();
            var memoryStream = ((LoggingSource.ForwardingStream)writer.BaseStream).Destination;
            MessageFactory.Invoke(memoryStream, Args);
        }

        public T Args { get; set; }
        public Action<MemoryStream, T> MessageFactory { get; set; }
    }
}
