extern alias client;
using Raven.Client.Exceptions;

using System.Collections.Generic;

using Raven.Client;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class Matthew : AuthorizationTest
    {
        [Fact]
        public void AuthorizationDemo_Works()
        {
            // Arrange
            using (IDocumentSession session = store.OpenSession(DatabaseName))
            {
                session.Store(
                    new client::Raven.Bundles.Authorization.Model.AuthorizationRole
                        {
                            Id = "Authorization/Roles/Nurses",
                            Permissions =
                                {
                                    new client::Raven.Bundles.Authorization.Model.OperationPermission
                                        {
                                            Allow = true,
                                            Operation = "Appointment/Schedule",
                                            Tags = new List<string> {"Patient"}
                                        }
                                }
                        });

                // Allow doctors to authorize hospitalizations
                session.Store(
                    new client::Raven.Bundles.Authorization.Model.AuthorizationRole
                        {
                            Id = "Authorization/Roles/Doctors",
                            Permissions =
                                {
                                    new client::Raven.Bundles.Authorization.Model.OperationPermission
                                        {
                                            Allow = true,
                                            Operation = "Hospitalization/Authorize",
                                            Tags = new List<string> {"Patient"}
                                        }
                                }
                        });
                // Associate Patient with clinic
                var maryMallon = new Patient {Id = "Patients/MaryMallon"};
                session.Store(maryMallon);
	            client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, maryMallon,
	                                                                                                 new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
		                                                                                                 {
			                                                                                                 Tags =
				                                                                                                 {
					                                                                                                 "Clinics/Kirya",
					                                                                                                 "Patient"
				                                                                                                 }
		                                                                                                 });

                // Associate Doctor with clinic
                session.Store(
                    new client::Raven.Bundles.Authorization.Model.AuthorizationUser
                        {
                            Id = "Authorization/Users/DrHowser",
                            Name = "Doogie Howser",
                            Roles = {"Authorization/Roles/Doctors"},
                            Permissions =
                                {
                                    new client::Raven.Bundles.Authorization.Model.OperationPermission
                                        {
                                            Allow = true,
                                            Operation = "Patient/View",
                                            Tags = new List<string> {"Clinics/Kirya"}
                                        },
                                }
                        });
                session.SaveChanges();
            }


            // Assert
            using (IDocumentSession session = store.OpenSession(DatabaseName))
            {
	            client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(session,
	                                                                                       "Authorization/Users/NotDrHowser",
	                                                                                       "Hospitalization/Authorize");
	            var readVetoException = Assert.Throws<ReadVetoException>(() => session.Load<Patient>("Patients/MaryMallon"));
	            Assert.Contains(
		            "Could not find user: Authorization/Users/NotDrHowser for secured document: Patients/MaryMallon",
		            readVetoException.Message);
            }
        }
    }

    public class Patient
    {
        public string Id { get; set; }

        public void AuthorizeHospitalization()
        {
        }
    }
}