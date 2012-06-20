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
			CreatedAt = SystemTime.Now;
		}

		public DateTime CreatedAt { get; private set; }
		public string Message { get; private set; }
		public NotificationLevel Level { get; private set; }

	    public string Details
	    {
	        get
	        {
	            var sb = new StringBuilder();

	            if (exception != null)
	            {
	                sb.Append(exception.ToString());
	            }

	            sb.AppendLine();
	            sb.AppendLine();

                if (details != null)
                {
                    foreach (var detail in details)
                    {
                        sb.AppendLine(detail.ToString());
                    }    
                }

	            return sb.ToString();
	        }
	    }
	}
}