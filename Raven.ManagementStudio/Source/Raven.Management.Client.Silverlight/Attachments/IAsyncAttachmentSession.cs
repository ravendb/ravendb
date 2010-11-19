namespace Raven.Management.Client.Silverlight.Attachments
{
    using System.Collections.Generic;
    using Common;
    using Database.Data;

    public interface IAsyncAttachmentSession
    {
        void Load(string key, CallbackFunction.Load<KeyValuePair<string, Attachment>> callback);

        void LoadMany(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback);

        void LoadPlugins(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback);
    }
}