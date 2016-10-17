using System;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Storage;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Commercial;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Database.Storage;
using Raven.Json.Linq;
using LogManager = Raven.Abstractions.Logging.LogManager;

namespace Raven.Database.Plugins.Builtins
{
    public class HotSpareReplicationBehavior : IServerStartupTask
    {
        public void Dispose()
        {
            DeactivateTimer();
            ValidateLicense.CurrentLicenseChanged -= OnCurrentLicenseChanged;
        }

        private void DeactivateTimer()
        {
            if (licensingTimer != null)
            {
                try
                {
                    landlord.SystemDatabase.TimerManager.ReleaseTimer(licensingTimer);
                }
                catch (InvalidOperationException)
                {
                    //We are trying to deactivate a timer that failed to register to the timer manager
                    // this should not happen but better safe than sorry
                    log.Warn("Disposing of a timer within HotSpareReplicationBehavior that failed to register to the timer manager");
                    licensingTimer.Dispose();
                }
            }
        }

        private DatabasesLandlord landlord;
        private RequestManager requestManger;
        private LicensingStatus licensingStatus;
        private readonly ILog log = LogManager.GetCurrentClassLogger();
        private readonly HttpJsonRequestFactory requestFactory = new HttpJsonRequestFactory(16);

        /// <summary>
        /// A static timer to disable databases when license expired.
        /// </summary>
        private volatile static Timer licensingTimer;

        /// <summary>
        /// A static timer to disable databases when license expired.
        /// </summary>
        private volatile static Timer pollingLicenseStateTimer;

        public void Execute(RavenDBOptions serverOptions)
        {
            requestManger = serverOptions.RequestManager;
            landlord = serverOptions.DatabaseLandlord;
            requestManger.HotSpareValidator = this;
            licensingStatus = GetLicensingStatus();
            ValidateLicense.CurrentLicenseChanged += OnCurrentLicenseChanged;
            CheckHotSpareLicenseStats();
        }

        private void OnCurrentLicenseChanged(LicensingStatus newLicense)
        {
            //don't want to do anything if the license is the same.
            if (LicenseEqual(licensingStatus, newLicense)) return;
            licensingStatus = newLicense;
            CheckHotSpareLicenseStats();
        }

        internal void ClearHotSpareData()
        {
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                action.Lists.RemoveAllOlderThan(HotSpareList,DateTime.MaxValue);
            });
        }

        public async Task ActivateHotSpareLicense()
        {
            var id = GetLicenseId();
            var now = SystemTime.UtcNow;
            if (string.IsNullOrEmpty(id))
            {
                log.Warn(noIdMessage);
                RaiseAlert(noIdMessage,NoIdTitle,AlertLevel.Warning);
                return;
            }

            var doc = GetOrCreateLicenseDocument(id);
            if (IsActivationExpired(doc))
            {
                log.Warn(multipleActivationMessage);
                RaiseAlert(multipleActivationMessage, multipleActivationTitle,AlertLevel.Warning);
                ReportLicensingUsage(id, ReportHotSpareUsage.ActivationMode.MultipleActivation);
                //allowing to reactivate hot spare so not to hurt the user
                doc.ActivationTime = now;
            }
            else if (doc.ActivationMode == HotSpareLicenseDocument.HotSpareLicenseActivationMode.NotActivated)
            {
                ReportLicensingUsage(id, ReportHotSpareUsage.ActivationMode.FirstActivation);
                doc.ActivationTime = now;
                doc.ActivationMode = HotSpareLicenseDocument.HotSpareLicenseActivationMode.Activated;
            }

            await ChangeHotSpareModeWithinCluster(false).ConfigureAwait(false);

            PutLicenseDocument(id, doc);
            DeactivateTimer();                        
            requestManger.IsInHotSpareMode = false;
            // next check time should be positive because we handle expired licensing already
            var nextCheckTime = ActivationTime - (now - doc.ActivationTime);
            licensingTimer = landlord.SystemDatabase.TimerManager.NewTimer(ActivationTimeoutCallback, nextCheckTime.Value, NonRecurringTimeSpan);
        }

        private async Task ChangeHotSpareModeWithinCluster(bool hotSpareMode)
        {
            //If we are in a cluster we want the leader to aprrove the activation
            //We don't want to save the license document before we are approved 
            var clusterManager = landlord.SystemDatabase?.ClusterManager?.Value;
            if (clusterManager != null && (clusterManager.Engine.CurrentTopology?.ToString() != Topology.EmptyTopology))
            {
                var res = await clusterManager.Client.SendVotingModeChangeRequestAsync(clusterManager.Engine.Options.SelfConnection, !hotSpareMode).ConfigureAwait(false);
                //We are in a cluster but we failed to be activated by the leader
                if (!res)
                {
                    throw new Exception($"Hot Spare server failed to be activated because the server is in a cluster and there is no quorum to add it.");
                }
                //updating the self connection to indicate the voting state of the node
                clusterManager.Engine.Options.SelfConnection.IsNoneVoter = !hotSpareMode;
            }
        }

        public bool IsActivationExpired(string id)
        {
            var doc = GetOrCreateLicenseDocument(id);
            return doc.ActivationMode == HotSpareLicenseDocument.HotSpareLicenseActivationMode.Activated && doc.ActivationTime.HasValue
                   && SystemTime.UtcNow - doc.ActivationTime.Value > ActivationTime;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsActivationExpired(HotSpareLicenseDocument doc)
        {
            return doc.ActivationMode == HotSpareLicenseDocument.HotSpareLicenseActivationMode.Activated && doc.ActivationTime.HasValue
                   && SystemTime.UtcNow - doc.ActivationTime.Value > ActivationTime;
        }

        private const string multipleActivationMessage = "Multiple activation of hot spare license detected.";
        private const string multipleActivationTitle = "Multiple hot spare activation";

        public async Task EnableTestModeForHotSpareLicense()
        {
            var id = GetLicenseId();
            if (string.IsNullOrEmpty(id))
            {
                log.Warn(noIdMessage);
                RaiseAlert(noIdMessage, NoIdTitle, AlertLevel.Warning);
                return;
            }

            if (requestManger.IsInHotSpareMode == false)
            {
                //we are already in not hot spare mode, nothing to do
                return;
            }

            var doc = GetOrCreateLicenseDocument(id);
            //If we are already running on an expired license block testing.
            if (IsActivationExpired(doc) || IsTestAllowanceOut(doc))
            {
                log.Warn(RanOutOfTestAllowanceMessage);
                RaiseAlert(RanOutOfTestAllowanceMessage,RanOutOfTestAllowanceTitle, AlertLevel.Warning);
                return;
            }

            await ChangeHotSpareModeWithinCluster(false).ConfigureAwait(false);
            doc.RemainingTestActivations--;
            PutLicenseDocument(id, doc);	
            DeactivateTimer();
            requestManger.IsInHotSpareMode = false;
            licensingTimer = landlord.SystemDatabase.TimerManager.NewTimer(TestTimeoutCallback, TestActivationTime, NonRecurringTimeSpan);
        }

        private const string RanOutOfTestAllowanceMessage = "You have ran out of test allowance for this hot spare instance.";
        private const string RanOutOfTestAllowanceTitle = "Out of test allowance for hot spare";
        private const string NoIdTitle = "No license id";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsTestAllowanceOut(HotSpareLicenseDocument doc)
        {
            return doc.RemainingTestActivations <= 0;
        }

        /// <summary>
        /// Invokes ChangeHotSpareModeWithinCluster and in the case of failure sets a timer to call 
        /// CheckHotSpareLicenseStats again within a minute.
        /// </summary>
        /// <param name="hotSpareMode">the state we want to set the hotSpare to</param>
        /// <returns>true if the state was changed without throwing</returns>
        private bool ChangeHotSpareModeWithinClusterForCheckHotSpareLicenseStats(bool hotSpareMode)
        {
            try
            {
                Abstractions.Util.AsyncHelpers.RunSync(() => ChangeHotSpareModeWithinCluster(hotSpareMode));
            }
            catch
            {
                if(pollingLicenseStateTimer == null)
                    //the reason we fail here is only because we are running within a cluster and could not get the leader to approve our voting state
                    pollingLicenseStateTimer = landlord.SystemDatabase.TimerManager.NewTimer(CheckHotSpareLicenseStats, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
                return false;
            }
            if (pollingLicenseStateTimer != null)
                try
                {
                    landlord.SystemDatabase.TimerManager.ReleaseTimer(pollingLicenseStateTimer);
                }
                catch (InvalidOperationException)
                {
                    //shouldn't happen...
                    pollingLicenseStateTimer.Dispose();
                }
            return true;
        }

        public void CheckHotSpareLicenseStats(object state = null)
        {
            var id = GetLicenseId();
            // Non-commercial license with hot spare history
            if (id == null && CheckForHotSpareFootprintAndReport())
            {
                if(ChangeHotSpareModeWithinClusterForCheckHotSpareLicenseStats(true) == false) 
                    return;
                requestManger.IsInHotSpareMode = true;
                return;
            }

            //Non-commercial with no hot spare usage.
            if (id == null)
                return;

            if (IsHotSpareLicense())
            {
                var doc = GetOrCreateLicenseDocument(id);
                if (IsActivationExpired(doc))
                {
                    log.Warn(ExpiredHotSpareLicensingUssageMessage);
                    RaiseAlert(ExpiredHotSpareLicensingUssageMessage,ExpiredHotSpareLicenseTitle, AlertLevel.Warning);
                    ReportLicensingUsage(id, ReportHotSpareUsage.ActivationMode.ExpiredActivation);
                    if (ChangeHotSpareModeWithinClusterForCheckHotSpareLicenseStats(true) == false)
                        return;

                    requestManger.IsInHotSpareMode = true;
                    return;
                }
                //Activated but not expired (would happen if server was down.
                if (doc.ActivationMode == HotSpareLicenseDocument.HotSpareLicenseActivationMode.Activated && doc.ActivationTime.HasValue)
                {
                    var expirationTime = ActivationTime - (SystemTime.UtcNow - doc.ActivationTime.Value);					
                    expirationTime = (expirationTime > TimeSpan.Zero) ? expirationTime : TimeSpan.Zero;
                    DeactivateTimer();
                    if (ChangeHotSpareModeWithinClusterForCheckHotSpareLicenseStats(false) == false)
                        return;

                    requestManger.IsInHotSpareMode = false;
                    licensingTimer = landlord.SystemDatabase.TimerManager.NewTimer(ActivationTimeoutCallback, expirationTime, NonRecurringTimeSpan);
                    return;
                }
                if (!ChangeHotSpareModeWithinClusterForCheckHotSpareLicenseStats(true))
                    return;
                //not activated or back from testing
                requestManger.IsInHotSpareMode = true;
                return;
            }

            //We are running on a comercial license need to clear hot spare footprint
            if (licensingStatus.IsCommercial)
            {
                ClearHotSpareData();
                requestManger.IsInHotSpareMode = false;
            }
        }
        
        private void ReportLicensingUsage(string id, ReportHotSpareUsage.ActivationMode mode)
        {
            Task.Run(async() =>
            {
                try
                {
                    var requestParam = new CreateHttpJsonRequestParams(null, "http://licensing.ravendb.net/hot-spare/activation", HttpMethod.Post, null, null, conventions);
                    using (var request = requestFactory.CreateHttpJsonRequest(requestParam))
                    {
                        await request.WriteAsync(
                            RavenJObject.FromObject(new ReportHotSpareUsage
                            {
                                LicenseId = id,
                                Mode = mode
                            })).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    log.WarnException("Failed to notify about hot sapre licensing usage.", e);
                }
            });
        }

        private readonly DocumentConvention conventions = new DocumentConvention();
        private string GetLicenseId()
        {
            string id;
            licensingStatus.Attributes.TryGetValue("UserId", out id);
            return id;
        }
        
        public HotSpareLicenseDocument GetOrCreateLicenseDocument(string id, bool checkIfTesting = false)
        {
            var doc = GetLicenseDocument(id) ?? CreateDefaultHotSpareLicenseDocument(id);
            if (checkIfTesting == false)
                return doc;

            if (requestManger.IsInHotSpareMode == false &&
                doc.ActivationMode == HotSpareLicenseDocument.HotSpareLicenseActivationMode.NotActivated)
            {
                //we are not in hot spare mode and in not activated activation mode
                doc.ActivationMode = HotSpareLicenseDocument.HotSpareLicenseActivationMode.Testing;
            }

            return doc;
        }

        private HotSpareLicenseDocument GetLicenseDocument(string id)
        {
            var docKey = GenerateHotSpareDocKey(id);

            ListItem listItem = null;
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                listItem = action.Lists.Read(HotSpareList, docKey);
            });

            if (listItem == null)
                return null;

            try
            {
                return listItem.Data.JsonDeserialization<HotSpareLicenseDocument>();
            }
            catch (Exception e)
            {
                log.WarnException(failedToDeserialize, e);
                return null;
            }
        }

        private void PutLicenseDocument(string id, HotSpareLicenseDocument doc)
        {
            var docKey = GenerateHotSpareDocKey(id);
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                action.Lists.Set(HotSpareList, docKey,RavenJObject.FromObject(doc),UuidType.Licensing);
            });
        }
        private const string noIdMessage = "Can't activate hot spare license because the license doesn't contain an id.";
        private const string failedToDeserialize = "Failed to deserialize license document.";

        private void RaiseAlert(string message,string title,AlertLevel alertLevel)
        {
            landlord.SystemDatabase.AddAlert(new Alert()
            {
                AlertLevel = alertLevel,
                CreatedAt = SystemTime.UtcNow,
                Message = message,
                Title = title

            });
        }
        private static string GenerateHotSpareDocKey(string id)
        {
            return $"{HotSpareKeyPrefix}/{id}";
        }

        public bool IsHotSpareLicense()
        {
            string isHotSpareStr;
            licensingStatus.Attributes.TryGetValue("HotSpare", out isHotSpareStr);
            bool isHotSpare;
            bool.TryParse(isHotSpareStr, out isHotSpare);
            return isHotSpare;
        }

        private bool CheckForHotSpareFootprintAndReport()
        {
            bool isHotSpareFootPrintFound = false;
            string id = null;
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                int start = 0;
                int taken;
                do
                {
                    var licenses = action.Lists.Read(HotSpareList, start, 10).ToArray();
                    taken = licenses.Length;
                    start += taken;
                    foreach (var license in licenses)
                    {
                        HotSpareLicenseDocument data;
                        try
                        {
                            data = license.Data.JsonDeserialization<HotSpareLicenseDocument>();
                        }
                        catch (Exception)
                        {
                            log.Warn(FailureToDeserializeHotSpareDocument);
                            continue;
                        }
                        //Nothing to report if i got no id...
                        if (string.IsNullOrEmpty(data.Id)) continue;
                        id = data.Id;
                        isHotSpareFootPrintFound = true;
                        break;
                    }
                    if (isHotSpareFootPrintFound) break;
                } while (taken != 0);
                    
                });
            if(isHotSpareFootPrintFound)
                ReportUsageOfExpiredHotSpareLicense(ReportHotSpareUsage.ActivationMode.WasHotSpareButNoHaveNoLicense,id);
            return isHotSpareFootPrintFound;
        }

        private const string FailureToDeserializeHotSpareDocument = "Failed to deserialzed hot spare document.";

        private void ReportUsageOfExpiredHotSpareLicense(ReportHotSpareUsage.ActivationMode mode, string licenseId)
        {
            RaiseAlert(ExpiredHotSpareLicensingUssageMessage, ExpiredHotSpareLicenseTitle,AlertLevel.Warning);
            log.Warn(ExpiredHotSpareLicensingUssageMessage);
            ReportLicensingUsage(licenseId, mode);
        }

        private const string ExpiredHotSpareLicensingUssageMessage = @"You're running a RavenDB expried Hot Spare Server. Please buy a new license at: http://ravendb.net/buy";
        private const string ExpiredHotSpareLicenseTitle = "Hot spare license expired";

        private void ActivationTimeoutCallback(object state)
        {
            var newLicense = GetLicensingStatus();
            DeactivateTimer();
            if (LicenseEqual(newLicense, licensingStatus))
            {
                try
                {
                    Abstractions.Util.AsyncHelpers.RunSync(()=>ChangeHotSpareModeWithinCluster(true));
                }
                catch
                {
                    //the reason we fail here is only because we are running within a cluster and could not get the leader to approve our voting state
                    licensingTimer = landlord.SystemDatabase.TimerManager.NewTimer(ActivationTimeoutCallback, TimeSpan.FromMinutes(1), NonRecurringTimeSpan);
                    return;
                }
                requestManger.IsInHotSpareMode = true;
                ReportUsageOfExpiredHotSpareLicense(ReportHotSpareUsage.ActivationMode.ExpiredActivation, licensingStatus.Attributes["UserId"]);				
                return;
            }
            licensingStatus = newLicense;
            CheckHotSpareLicenseStats();
        }

        private void TestTimeoutCallback(object state)
        {
            licensingStatus = GetLicensingStatus();
            DeactivateTimer();
            try
            {
                Abstractions.Util.AsyncHelpers.RunSync(() => ChangeHotSpareModeWithinCluster(true));
            }
            catch
            {
                //the reason we fail here is only because we are running within a cluster and could not get the leader to approve our voting state
                licensingTimer = landlord.SystemDatabase.TimerManager.NewTimer(TestTimeoutCallback, TimeSpan.FromMinutes(1), NonRecurringTimeSpan);
                return;
            }

            requestManger.IsInHotSpareMode = true;
            CheckHotSpareLicenseStats();
        }

        private static HotSpareLicenseDocument CreateDefaultHotSpareLicenseDocument(string id)
        {
            return
                new HotSpareLicenseDocument
                {
                    Id = id,
                    ActivationMode = HotSpareLicenseDocument.HotSpareLicenseActivationMode.NotActivated,
                    ActivationTime = null,
                    RemainingTestActivations = MaxTestAllowance
                };
        }

        private LicensingStatus GetLicensingStatus()
        {
            return ValidateLicense.CurrentLicense;
        }

        public class HotSpareLicenseDocument
        {
            public string Id { get; set; }

            public HotSpareLicenseActivationMode ActivationMode { get; set; }

            public DateTime? ActivationTime { get; set; }

            public int RemainingTestActivations { get; set; }

            public enum HotSpareLicenseActivationMode
            {
                NotActivated,
                Activated,
                Testing
            }
        }

        private class ReportHotSpareUsage
        {
            public string LicenseId { get; set; }

            public ActivationMode Mode { get; set; }

            public enum ActivationMode
            {
                FirstActivation,
                ExpiredActivation,
                MultipleActivation,
                WasHotSpareButNoHaveNoLicense
            }
        }

        private static bool LicenseEqual(LicensingStatus license1, LicensingStatus license2)
        {
            string id1;
            string id2;
            license1.Attributes.TryGetValue("UserId", out id1);
            license2.Attributes.TryGetValue("UserId", out id2);
            return (id1 == id2 && license1.Status == license2.Status);
        }
        
        private const int MaxTestAllowance = 500;
        private static readonly TimeSpan TestActivationTime = TimeSpan.FromHours(1);			
        private static readonly TimeSpan ActivationTime = TimeSpan.FromHours(96);
        private static readonly TimeSpan NonRecurringTimeSpan = TimeSpan.FromMilliseconds(-1);
        private const string HotSpareList = "HotSpare";
        private const string HotSpareKeyPrefix = "Raven/HotSpareLicenseState";
    }
}
