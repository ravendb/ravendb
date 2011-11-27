using System;
using System.ServiceModel;

namespace Rhino.Licensing
{
    /// <summary>
    /// Service contract of the licensing server.
    /// </summary>
    [ServiceContract(SessionMode = SessionMode.NotAllowed)]
    public interface ILicensingService
    {
        /// <summary>
        /// Issues a float license for the user.
        /// </summary>
        /// <param name="machine">machine name</param>
        /// <param name="user">user name</param>
        /// <param name="id">Id of the license holder</param>
        /// <returns></returns>
        [OperationContract]
        string LeaseLicense(string machine, string user, Guid id);
    }
}