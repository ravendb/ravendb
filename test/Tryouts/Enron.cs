using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MimeKit;

namespace Tryouts
{
    public static class Enron
    {
        public const string DatasetFile = @"C:\Users\Oren\Downloads\enron_mail_20150507.tar.gz";
    }

    public class Message
    {
        public MessagePriority Priority { get; set; } 
        public XMessagePriority XPriority { get; set; }

        public string Sender { get; set; } 
        public List<string> From { get; set; } 
        public List<string> ReplyTo { get; set; } 
        public List<string> To { get; set; }
        public List<string> Cc { get; set; }
        public List<string> Bcc { get; set; }

        public MessageImportance Importance { get; set; }
        public string Subject { get; set; } 

        public DateTimeOffset Date { get; set; }
        public List<string> References { get; set; }
        public string InReplyTo { get; set; }
        public string MessageId { get; set; }
        public string TextBody { get; set; }

        public Dictionary<string, List<string>> Headers { get; set; }
    }
}
