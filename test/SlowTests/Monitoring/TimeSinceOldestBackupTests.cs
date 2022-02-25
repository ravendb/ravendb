using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Monitoring
{
    public class TimeSinceOldestBackupTests : RavenTestBase
    {
        private readonly Dictionary<string, DateTime?> _backupDates;
        private readonly SystemTime _time;

        public TimeSinceOldestBackupTests(ITestOutputHelper output) : base(output)
        {
            _backupDates = new Dictionary<string, DateTime?>();
            _time = new SystemTime();
            _time.UtcDateTime = () => new DateTime(2022, 02, 15);
        }

        private void SetupDatabasesBackupTimes(DateTime?[] backupDates)
        {
            for (var i = 0; i < backupDates.Length; i++)
            {
                _backupDates.Add(i.ToString(), backupDates[i]);
            }
        }

        private DateTime? GetLastBackupDate(string databaseName)
        {
            return _backupDates[databaseName];
        }

        [Fact]
        public void GivenNeverBackedUpDb_ReturnsTimespanMax()
        {
            var backupTimes = new DateTime?[]
            {
                _time.GetUtcNow().Date, 
                _time.GetUtcNow().Subtract(TimeSpan.FromDays(1)), 
                _time.GetUtcNow(),
                DateTime.MinValue, 
            };
            
            SetupDatabasesBackupTimes(backupTimes);
            
            var result = 
                DatabaseOldestBackup.GetTimeSinceOldestBackupInternal(_backupDates.Keys.ToList(), GetLastBackupDate, _time);
            Assert.True(result > SnmpValuesHelper.TimeSpanSnmpMax);
            Assert.Equal(SnmpValuesHelper.TimeTicksMax, SnmpValuesHelper.TimeSpanToTimeTicks(result));
        } 
        
        [Fact]
        public void GivenDbJustBackedUp_ReturnsZero()
        {
            var backupTimes = new DateTime?[]
            {
                _time.GetUtcNow(), 
            };
            
            SetupDatabasesBackupTimes(backupTimes);
            
            var result = 
                DatabaseOldestBackup.GetTimeSinceOldestBackupInternal(_backupDates.Keys.ToList(), GetLastBackupDate, _time);
            Assert.Equal(TimeSpan.Zero, result);
            Assert.Equal(SnmpValuesHelper.TimeTicksZero,
                SnmpValuesHelper.TimeSpanToTimeTicks(result));
            Assert.Equal(SnmpValuesHelper.TimeTicksZero, SnmpValuesHelper.TimeSpanToTimeTicks(result));
        } 
        
        [Fact]
        public void Given_NoDbs_ReturnsZero()
        {
            var backupTimes = new DateTime?[] {};
            
            SetupDatabasesBackupTimes(backupTimes);
            
            var result = 
                DatabaseOldestBackup.GetTimeSinceOldestBackupInternal(_backupDates.Keys.ToList(), GetLastBackupDate, _time);
            Assert.Equal(TimeSpan.Zero, result);
            Assert.Equal(SnmpValuesHelper.TimeTicksZero, SnmpValuesHelper.TimeSpanToTimeTicks(result));
        } 
        
        [Fact]
        public void GivenAFewDatabases_ShouldProvideDurationSinceTheOldestDatabaseBackup()
        {
            var backupTimes = new DateTime?[]
            {
                _time.GetUtcNow().Date, 
                _time.GetUtcNow().Subtract(TimeSpan.FromDays(1)), 
                _time.GetUtcNow(),
                null
            };
            
            SetupDatabasesBackupTimes(backupTimes);
            
            var result = 
                DatabaseOldestBackup.GetTimeSinceOldestBackupInternal(_backupDates.Keys.ToList(), GetLastBackupDate, _time);
            Assert.Equal(_time.GetUtcNow() - backupTimes[1], result);
        } 
        
        [Fact]
        public void GivenDatabaseBackedUp_MoreThanMaxTimeTicksAgo_ReturnsTimespanMax()
        {
            var maxTimeTicksTimespan = SnmpValuesHelper.TimeTicksMax.ToTimeSpan();
            var backupTimes = new DateTime?[]
            {
                _time.GetUtcNow().Subtract(maxTimeTicksTimespan).Subtract(TimeSpan.FromDays(1))
            };
            
            SetupDatabasesBackupTimes(backupTimes);
            
            var result = 
                DatabaseOldestBackup.GetTimeSinceOldestBackupInternal(_backupDates.Keys.ToList(), GetLastBackupDate, _time);
            Assert.True(result > SnmpValuesHelper.TimeSpanSnmpMax);
            Assert.Equal(SnmpValuesHelper.TimeTicksMax, SnmpValuesHelper.TimeSpanToTimeTicks(result));
        } 
    }
}
