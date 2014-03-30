package net.ravendb.tests.indexes;

import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.indexes.QIndexWithSubPropertyTest_Contact;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class IndexWithSubPropertyTest  {
  @Test
  public void indexWithSubPropertyReturnAs_Property_SubProperty() throws Exception {
    ContactIndex contactIndex = new ContactIndex();
    contactIndex.setConventions(new DocumentConvention());
    IndexDefinition result = contactIndex.createIndexDefinition();

    assertTrue(result.getStores().containsKey("PrimaryEmail_Email"));
    assertTrue(result.getIndexes().containsKey("PrimaryEmail_Email"));
    assertTrue(result.getAnalyzers().containsKey("PrimaryEmail_Email"));
    assertTrue(result.getStores().containsKey("String_Store"));
    assertTrue(result.getIndexes().containsKey("String_Index"));
    assertTrue(result.getAnalyzers().containsKey("String_Analyzer"));


  }

  public static class ContactIndex extends AbstractIndexCreationTask {
    public ContactIndex() {
      map = "from contact in docs.contacts " +
      		"select new " +
      		"{ " +
      		"  contat.FirstName, " +
      		"  PrimaryEmail_EmailAddress = contact.PrimaryEmail.Email " +
      		"};";

      QIndexWithSubPropertyTest_Contact x = QIndexWithSubPropertyTest_Contact.contact;
      store("String_Store", FieldStorage.YES);
      store(x.primaryEmail.email, FieldStorage.YES);
      index(x.primaryEmail.email, FieldIndexing.ANALYZED);
      index("String_Index", FieldIndexing.ANALYZED);
      analyze(x.primaryEmail.email, "SimpleAnalyezer");
      analyze("String_Analyzer", "SnowballAnalyzer");
    }
  }

  @QueryEntity
  public static class Contact {
    private String id;
    private String firstName;
    private String surname;
    private EmailAddress primaryEmail;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getFirstName() {
      return firstName;
    }
    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }
    public String getSurname() {
      return surname;
    }
    public void setSurname(String surname) {
      this.surname = surname;
    }
    public EmailAddress getPrimaryEmail() {
      return primaryEmail;
    }
    public void setPrimaryEmail(EmailAddress primaryEmail) {
      this.primaryEmail = primaryEmail;
    }
  }

  @QueryEntity
  public static class EmailAddress {
    private String email;

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

  }
}
