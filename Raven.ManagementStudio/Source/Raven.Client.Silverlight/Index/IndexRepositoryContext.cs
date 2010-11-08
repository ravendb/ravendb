namespace Raven.Client.Silverlight.Index
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Raven.Client.Silverlight.Common;

    public class IndexRepositoryContext<T> : IRepositoryContext where T : Data.Index
    {
        public IndexRepositoryContext(CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback, object state)
        {
            this.Callback = callback;
            this.StoreCallback = storeCallback;
            this.State = state;
            this.Context = SynchronizationContext.Current;
        }

        public IndexRepositoryContext(CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback, object state)
        {
            this.CallbackCollection = callback;
            this.StoreCallbackCollection = storeCallback;
            this.State = state;
            this.Context = SynchronizationContext.Current;
        }

        public IndexRepositoryContext(CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback, object state)
        {
            this.SaveCallback = callback;
            this.StoreCallback = storeCallback;
            this.State = state;
            this.Context = SynchronizationContext.Current;
        }

        public object State { get; private set; }

        private SynchronizationContext Context { get; set; }

        private CallbackFunction.Load<T> Callback { get; set; }

        private CallbackFunction.Load<IList<T>> CallbackCollection { get; set; }

        private CallbackFunction.Store<T> StoreCallback { get; set; }

        private CallbackFunction.Store<IList<T>> StoreCallbackCollection { get; set; }

        private CallbackFunction.Save SaveCallback { get; set; }

        public void Post(object entity)
        {
            if (this.StoreCallback != null)
            {
                this.Context.Post(
                    delegate
                    {
                        var save = entity as SaveResponse;
                        if (save != null)
                        {
                            this.StoreCallback.Invoke(save.Entity as T);
                        }
                        else
                        {
                            this.StoreCallback.Invoke((T)entity);
                        }
                    },
                    null);
            }

            if (this.StoreCallbackCollection != null)
            {
                this.Context.Post(
                    delegate
                    {
                        var objects = (List<object>)entity;
                        this.StoreCallbackCollection.Invoke(objects.Cast<T>().ToList());
                    },
                    null);
            }

            if (this.Callback != null)
            {
                this.Context.Post(
                    delegate
                    {
                        this.Callback.Invoke((T)entity);
                    },
                    null);
            }

            if (this.CallbackCollection != null)
            {
                this.Context.Post(
                    delegate
                    {
                        var objects = (List<object>)entity;
                        this.CallbackCollection.Invoke(objects.Cast<T>().ToList());
                    },
                    null);
            }

            if (this.SaveCallback != null)
            {
                this.Context.Post(
                    delegate
                    {
                        this.SaveCallback.Invoke(entity as SaveResponse);
                    },
                    null);
            }
        }
    }
}
