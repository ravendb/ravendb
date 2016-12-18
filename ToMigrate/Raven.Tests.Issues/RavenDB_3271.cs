using System;
using System.Collections.Generic;
using Raven.Bundles.Authorization.Model;
using Raven.Client;
using Raven.Client.Authorization;
using Raven.Client.Exceptions;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Tests.Bundles.Authorization;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class SanityCheck : AuthorizationTest
{

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            Authentication.EnableOnce();
            configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
        }

        [Fact]
        public void IsAuthorizationWorking()
        {
            const string ptId = "testuser-Patients-MaryMallon";
            const string docId = "testdoctor-Doctors-DrHowser";
            const string roleId = "test-roleAuthorization-Roles-Doctors";            
     using (var session = store.OpenSession())
             {
                
                     // Allow doctors to authorize hospitalizations
                     session.Store(
                         new AuthorizationRole
                         {
                             Id = roleId,
                             Permissions =
                             {
                                 new OperationPermission
                                 {
                                     Allow = true,
                                     Operation = "Hospitalization/Authorize",
                                     Tags = new List<string> {"Patient"}
                                 }
                             }
                         });

                     // Associate Patient with clinic
                     var maryMallon = new User() {Id = ptId};
                     session.Store(maryMallon);
                     session.SetAuthorizationFor(maryMallon, new DocumentAuthorization
                     {
                         Tags =
                         {
                             "Clinics/Kirya",
                             "Patient"
                         }
                     });

                     var drHowser = new User()
                     {
                         Id = docId,
                         Name = "Doogie Howser",
                         Roles = new List<string>(){roleId},
                         Permissions = new List<OperationPermission>()
                         {
                             new OperationPermission
                             {
                                 Allow = true,
                                 Operation = "Patient/View",
                                 Tags = new List<string> {"Clinics/Kirya"}
                             }
                         }
                     };
                     // Associate Doctor with clinic
                     session.Store(drHowser);
                     session.SaveChanges();
                     //Clear session as second level cached documents bypass the cache
                     session.Advanced.Clear();
                     //Test allowed operation
                     //WaitForUserToContinueTheTest(store);
                     session.SecureFor(drHowser.Id, "Patient/View");
                     
                         maryMallon = session.Load<User>(maryMallon.Id);
                    


                     //Clear session as second level cached documents bypass the cache
                     session.Advanced.Clear();

                     session.SecureFor(drHowser.Id, "NonexistingOp");
                     Assert.Throws(typeof(ReadVetoException), () => session.Load<User>(maryMallon.Id));


                 
             }     
        }
         public class User
         {
             public string Id { get; set; }
             public string Name { get; set; }
             public List<string> Roles { get; set; }
             public List<OperationPermission> Permissions { get; set; }
         }
    }


}
