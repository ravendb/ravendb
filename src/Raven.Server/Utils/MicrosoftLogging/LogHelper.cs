using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class LogHelper
{
    public static void InterpolateDirectly(this MemoryStream memoryStream, [InterpolatedStringHandlerArgument("memoryStream")]DirectlyToStreamInterpolatedStringHandler format)
    {
        format.Clear();
    }
    
    public static void UseArrayPool(this Logger logger, [InterpolatedStringHandlerArgument("logger")]ArrayPoolBufferInterpolatedStringHandlerInfo format)
    {
        if (logger.IsInfoEnabled == false)
            return;
        var logEntry = new ByteArrayLoggerEntry { ByteArrayMessage = format.Text };
        logger.Info(ref logEntry);
        format.Clear();
    }
    public static void UseArrayPool(this Logger logger, Span<char> initBuffer, [InterpolatedStringHandlerArgument("logger", "initBuffer")]ArrayPoolBufferInterpolatedStringHandlerInfo format)
    {
        if (logger.IsInfoEnabled == false)
            return;
        var logEntry = new ByteArrayLoggerEntry { ByteArrayMessage = format.Text };
        logger.Info(ref logEntry);
        format.Clear();
    }
    
    struct ByteArrayLoggerEntry :ILogEntry
    {
        public DateTime At { get; set; }
        public LogMode Type { get; set; }
        public string Source { get; set; }
        public string Logger { get; set; }
        public string Message { get; set; }
        public Exception Exception { get; set; }

        public ArraySegment<char> ByteArrayMessage { get; set; }
        
        public void WriteMessage(StreamWriter writer)
        {
            writer.Write(ByteArrayMessage.Array, ByteArrayMessage.Offset, ByteArrayMessage.Count);
        }
    }
}
