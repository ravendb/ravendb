namespace Raven.Client.Silverlight.Common
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Data;

    public interface IRepositoryContext
    {
        object State { get; }
        void Post<T>(Response<T> response) where T : Entity;
        void Post<T>(Response<IList<T>> response) where T : Entity;
    }
}
