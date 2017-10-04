// -----------------------------------------------------------------------
//  <copyright file="SupportCoverage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Server.Tenancy;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Commercial
{
    public class SupportCoverage : IServerStartupTask
    {
        private const string SupportCoverageList = "SupportCoverage";
        private const string SupportCoverageKeyPrefix = "Raven/SupportCoverageState";
        private static readonly TimeSpan OneDay = TimeSpan.FromDays(1);

        private static volatile Timer supportTimer;
        public static SupportCoverageResult CurrentSupport { get; private set; } = CreateDefaultSupportCoverageDocument();

        private DatabasesLandlord landlord;
        private LicensingStatus licensingStatus;

        private readonly ILog log = LogManager.GetCurrentClassLogger();
        private readonly HttpJsonRequestFactory requestFactory = new HttpJsonRequestFactory(16);
        private readonly DocumentConvention conventions = new DocumentConvention();

        public void Dispose()
        {
            DeactivateTimer();
            ValidateLicense.CurrentLicenseChanged -= OnCurrentLicenseChanged;
        }

        private void DeactivateTimer()
        {
            var copy = supportTimer;
            if (copy == null)
                return;

            try
            {
                landlord?.SystemDatabase.TimerManager.ReleaseTimer(copy);
                supportTimer = null;
            }
            catch (InvalidOperationException)
            {
                // We are trying to deactivate a timer that failed to register to the timer manager
                // this should not happen but better safe than sorry
                log.Warn("Disposing of a timer within " + nameof(SupportCoverage) + " that failed to register to the timer manager");
                copy.Dispose();
            }
        }

        public void Execute(RavenDBOptions serverOptions)
        {
            landlord = serverOptions.DatabaseLandlord;
            licensingStatus = GetLicensingStatus();
            ValidateLicense.CurrentLicenseChanged += OnCurrentLicenseChanged;
            CheckSupportCoverage();
            supportTimer = landlord.SystemDatabase.TimerManager.NewTimer(_ => CheckSupportCoverage(), OneDay, OneDay);
        }

        private void CheckSupportCoverage()
        {
            var licenseId = GetLicenseId();
            // non-commercial
            if (licenseId == null)
            {
                return;
            }


            SupportCoverageResult supportDoc;
            try
            {
                //try using fresh copy
                supportDoc = QueryForSupportCoverage(licenseId);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to obtain support coverage information.", e);
                // use persisted copy if still valid
                supportDoc = GetSupportDocument(licenseId);
                if (supportDoc == null || supportDoc.EndsAt < SystemTime.UtcNow)
                {
                    supportDoc = CreateDefaultSupportCoverageDocument();
                }
            }
            PutSupportCoverage(licenseId, supportDoc);
            CurrentSupport = supportDoc;
        }

        private void PutSupportCoverage(string id, SupportCoverageResult doc)
        {
            var docKey = GenerateSupportStatusDocKey(id);
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                action.Lists.Set(SupportCoverageList, docKey, RavenJObject.FromObject(doc), UuidType.SupportCoverage);
            });
        }

        private SupportCoverageResult QueryForSupportCoverage(string id)
        {
            try
            {
                var requestParam = new CreateHttpJsonRequestParams(null, "http://licensing.ravendb.net/license/support/" + id, HttpMethod.Get, null, null, conventions);
                using (var request = requestFactory.CreateHttpJsonRequest(requestParam))
                {
                    var value = request.ReadResponseJson();
                    return conventions.CreateSerializer().Deserialize<SupportCoverageResult>(new RavenJTokenReader(value));
                }
            }
            catch (Exception e)
            {
                log.WarnException("Failed to obtain support coverage information.", e);
                return CreateDefaultSupportCoverageDocument();
            }
        }

        private static SupportCoverageResult CreateDefaultSupportCoverageDocument()
        {
            return new SupportCoverageResult
            {
                Status = SupportCoverageStatus.NoSupport,
                EndsAt = null
            };
        }

        private LicensingStatus GetLicensingStatus()
        {
            return ValidateLicense.CurrentLicense;
        }

        private void OnCurrentLicenseChanged(LicensingStatus newLicense)
        {
            //don't want to do anything if the license is the same.
            if (LicenseEqual(licensingStatus, newLicense)) return;
            licensingStatus = newLicense;
            DeactivateTimer();
            CheckSupportCoverage();
            supportTimer = landlord.SystemDatabase.TimerManager.NewTimer(_ => CheckSupportCoverage(), OneDay, OneDay);
        }

        private bool LicenseEqual(LicensingStatus license1, LicensingStatus license2)
        {
            string id1;
            string id2;
            license1.Attributes.TryGetValue("UserId", out id1);
            license2.Attributes.TryGetValue("UserId", out id2);
            return (id1 == id2 && license1.Status == license2.Status);
        }

        private string GetLicenseId()
        {
            string id;
            licensingStatus.Attributes.TryGetValue("UserId", out id);
            return id;
        }

        private SupportCoverageResult GetSupportDocument(string id)
        {
            var docKey = GenerateSupportStatusDocKey(id);
            ListItem listItem = null;
            landlord.SystemDatabase.TransactionalStorage.Batch(action =>
            {
                listItem = action.Lists.Read(SupportCoverageList, docKey);
            });
            try
            {
                return listItem?.Data.JsonDeserialization<SupportCoverageResult>();
            }
            catch (Exception)
            {
                log.Warn("Failed to deserialize support coverage document");
                return null;
            }
        }

        private string GenerateSupportStatusDocKey(string id)
        {
            return string.Format("{0}/{1}", SupportCoverageKeyPrefix, id);
        }
    }
    
    public enum SupportCoverageStatus
    {
        NoSupport,

        PartialSupport,

        ProfessionalSupport,

        ProductionSupport,

        LicenseNotFound,

        InvalidStateSupportNotFound,
    }

    public class SupportCoverageResult
    {
        public SupportCoverageStatus Status { get; set; }

        public DateTimeOffset? EndsAt { get; set; }
    }
}