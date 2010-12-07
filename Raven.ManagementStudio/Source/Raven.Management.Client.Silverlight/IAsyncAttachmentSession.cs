namespace Raven.Management.Client.Silverlight
{
    using System.Collections.Generic;
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Common;

    public interface IAsyncAttachmentSession
    {
        void Load(string key, CallbackFunction.Load<KeyValuePair<string, Attachment>> callback);

        void LoadMany(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback);

        void LoadPlugins(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback);
    }
}