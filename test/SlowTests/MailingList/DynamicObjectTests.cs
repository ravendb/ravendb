using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class DynamicObjectTests : RavenTestBase
    {
        /// <summary>
        /// this test works but really slow
        /// and if you turn on "Break on all Errors", you'll see
        /// some interesting exceptions
        /// </summary>
        [Fact]
        public void GetAllRecordsWithXtraFields()
        {
            using (var store = GetDocumentStore())
            {
                new FakeObjsIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    CreateTestRecords(session, "mb1123", "mbriggs", true);
                    WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var sw = new Stopwatch();
                    sw.Start();

                    var records = session.Query<FinalFakeObj>();

                    sw.Stop();
                    var ts = sw.Elapsed;
                    Assert.Equal<bool>(true, (records.Count() == 50));
                    Assert.NotInRange<double>(ts.TotalMilliseconds, 1200, 30000);
                }
            }
        }

        [Fact]
        public void GetAllRecordsWithOutXtraFields()
        {
            using (var store = GetDocumentStore())
            {
                new FakeObjsIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    CreateTestRecords(session, "mb1123", "mbriggs", false);
                    WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var sw = new Stopwatch();
                    sw.Start();

                    var records = session.Query<FinalFakeObj>();

                    sw.Stop();
                    var ts = sw.Elapsed;
                    Assert.Equal<bool>(true, (records.Count() == 50));
                    Assert.NotInRange<double>(ts.TotalMilliseconds, 1200, 30000);
                }
            }
        }

        private void CreateTestRecords(IDocumentSession session, string userId, string userName, bool addDynamicFlds)
        {
            for (int i = 0; i < 50; i++)
            {
                var indexer = i;
                FinalFakeObj r = new FinalFakeObj();

                //r.Id = "ffos/" + indexer + 1;
                r.RecordNum = "CA-" + indexer;
                r.Subject = "Testing Record " + indexer;
                r.DateDue = DateTime.Today.AddDays(indexer + 1);
                r.ResponsibleGroupId = userId;
                r.OriginatorUserId = userId;
                r.CoordinatorUserId = userId;
                r.LastUpdatedBy = new FakeDynamicRecordBase.RecBaseUser { Id = userId, Name = userName };

                if (addDynamicFlds)
                {
                    r.SetValue("StrFld1", "Emergency");
                    r.SetValue("StrFld2", "SomeStringValue");
                    r.SetValue("IntFld1", 1);
                    r.SetValue("IntFld2", 2);
                    r.SetValue("DecimalFld1", 1.44);
                }
                session.Store(r);
            }
            session.SaveChanges();
        }


        private abstract class FakeDynamicRecordBase : System.Dynamic.DynamicObject
        {
            public event PropertyChangedEventHandler PropertyChanged;
            private Dictionary<string, object> vals = new Dictionary<string, object>();

            public FakeDynamicRecordBase() : this(null) { }

            public FakeDynamicRecordBase(string configName)
            {
                this.ConfigName = configName ?? this.GetType().Name;
                this.LastUpdatedBy = new RecBaseUser();
                this.DateCreated = DateTime.Today;
                this.DateLastUpdated = DateTime.Now;
            }

            public string Id { get; set; }
            public virtual string RecordNum { get; set; }
            public virtual DateTime DateCreated { get; set; }
            public virtual DateTime DateLastUpdated { get; set; }
            public virtual bool Disabled { get; set; }
            public virtual string StateId { get; set; }
            public virtual int ConfigVersion { get; set; }
            public virtual string ConfigName { get; set; }
            public virtual RecBaseUser LastUpdatedBy { get; set; }

            private void NotifyPropertyChanged(String propertyName = "")
            {
                if (PropertyChanged != null)
                {
                    PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
                }
            }
            #region The DynamicObject Overrides plus helpers
            public virtual void SetValue(string key, dynamic newPropValue)
            {
                var myObjProp = GetObjectMember(key);
                if (myObjProp != null)
                {
                    myObjProp.SetValue(this, newPropValue, null);
                    NotifyPropertyChanged(key);
                }
                else
                {
                    //throw new MissingMemberException(this.GetType().Name, key);
                    //just add this key/value to props
                    vals[key] = newPropValue;
                }
            }
            public virtual dynamic GetValue(string key)
            {
                dynamic myVal = null;

                var myObjProp = GetObjectMember(key);
                if (myObjProp != null)
                    myVal = myObjProp.GetValue(this, null);
                else
                    vals.TryGetValue(key, out myVal);

                return myVal;
            }

            public virtual PropertyInfo GetObjectMember(string key)
            {
                var myObjType = this.GetType();
                var myObjProp = myObjType.GetProperty(key);
                return myObjProp;
            }

            public override bool TryGetMember(GetMemberBinder binder, out object result)
            {
                return vals.TryGetValue(binder.Name, out result);
            }

            public override bool TrySetMember(SetMemberBinder binder, object value)
            {
                if (binder.Name == "Id")
                    return false;
                vals[binder.Name] = value;
                return true;
            }

            public override bool TrySetIndex(SetIndexBinder binder, object[] indexes, object value)
            {
                var key = (string)indexes[0];
                if (key == "Id")
                    return false;
                vals[key] = value;
                return true;
            }

            public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
            {
                return vals.TryGetValue((string)indexes[0], out result);
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                return GetType().GetProperties().Select(x => x.Name).Concat(vals.Keys);
            }
            #endregion

            #region Nested Classes
            public class RecBaseUser
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public override string ToString()
                {
                    return Name;
                }
            }
            #endregion

        }

        private abstract class FakeSecondLevelBase : FakeDynamicRecordBase
        {
            public enum AgeReturnUnits
            {
                Seconds,
                Minutes,
                Hours,
                Days
            }

            [Flags]
            public enum SecurityLevels
            {
                None = 0,
                Level1 = 1,
                Level2 = 2,
                Level3 = 4,
                Level4 = 8
            }

            public FakeSecondLevelBase(string configName) : base(configName) { }

            public int Dash { get; set; }
            public int Item { get; set; }
            public string Subject { get; set; }
            public string ResponsibleGroupId { get; set; }
            public string OriginatorUserId { get; set; }
            public string CoordinatorUserId { get; set; }
            public DateTimeOffset? DateDue { get; set; }
            public DateTimeOffset? DateClosed { get; set; }
            public DateTimeOffset? DateAccepted { get; set; }
            public DateTimeOffset? DateCompleted { get; set; }
            public DateTimeOffset? DateValidated { get; set; }
            public SecurityLevels SecurityLevel { get; private set; }
            public bool MailSent { get; set; }

            public int GetNextDash()
            {
                //dash genreation logic here
                return 0;
            }

            public void SetSecurityLevel(SecurityLevels level)
            {
                this.SecurityLevel = level;
            }
            public int RecordAge(AgeReturnUnits returnUnit)
            {
                var diff = DateTime.Today.Subtract(this.DateCreated);
                int age;
                switch (returnUnit)
                {
                    case AgeReturnUnits.Seconds:
                        age = diff.Seconds;
                        break;
                    case AgeReturnUnits.Minutes:
                        age = diff.Minutes;
                        break;
                    case AgeReturnUnits.Hours:
                        age = diff.Hours;
                        break;
                    case AgeReturnUnits.Days:
                        age = diff.Days;
                        break;
                    default:
                        age = diff.Days;
                        break;
                }
                return age;
            }
        }

        private class FinalFakeObj : FakeSecondLevelBase
        {
            public FinalFakeObj() : base("FinalFakeObj") { }
        }

        private class FakeObjsIndex : AbstractMultiMapIndexCreationTask
        {
            public FakeObjsIndex()
            {
                AddMapForAll<FakeSecondLevelBase>(eas => from ea in eas
                                                         select new
                                                         {
                                                             ea.Id,
                                                             ea.Subject,
                                                             ea.ConfigName,
                                                             ea.RecordNum,
                                                             ea.ResponsibleGroupId,
                                                             ea.DateCreated,
                                                             ea.DateDue,
                                                             ea.DateClosed
                                                         });
            }
        }

    }
}
