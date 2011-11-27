using System.ServiceModel;

namespace Rhino.Licensing
{
    /// <summary>
    /// Service contract of subscription server.
    /// </summary>
    [ServiceContract]
    public interface ISubscriptionLicensingService
    {
        /// <summary>
        /// Issues a leased license
        /// </summary>
        /// <param name="previousLicense"></param>
        /// <returns></returns>
        [OperationContract]
        string LeaseLicense(string previousLicense);
    }
}