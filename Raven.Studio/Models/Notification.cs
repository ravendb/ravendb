using System;
using System.Text;
using Raven.Abstractions;

namespace Raven.Studio.Messages
{
	public class Notification
	{
	    private readonly Exception exception;
	    private readonly object[] details;

	    public Notification(string message, NotificationLevel level = NotificationLevel.Info, Exception exception = null, object[] details = null)
		{
		    this.exception = exception;
	        this.details = details;
	        Message = message;
			Level = level;
			CreatedAt = SystemTime.UtcNow;
		    RepeatCount = 1;
		}

		public DateTime CreatedAt { get; private set; }
		public string Message { get; private set; }
		public int RepeatCount { get; set; }

	    public NotificationLevel Level { get; private set; }

	    public string Details
	    {
	        get
	        {
	            var sb = new StringBuilder();

				if (details != null)
                {
                    foreach (var detail in details)
                    {
	                    if (detail == null)
		                    continue;
                        sb.AppendLine(detail.ToString());
                    }    
                }

				sb.AppendLine();
				sb.AppendLine();

				if (exception != null)
				{
					sb.AppendLine("Client side exception:");
					sb.Append(exception);
				}

	            return sb.ToString();
	        }
	    }
	}
}