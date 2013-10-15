package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.linq.QCommitInfo;

import org.junit.Test;


public class UsingWhereConditionsTest extends RemoteClientTest {

  @Test
  public void can_Use_Where() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      String indexName = "CommitByRevision";

      try (IDocumentSession session = store.openSession()) {
        addData(session);

        store.getDatabaseCommands().deleteIndex(indexName);
        IndexDefinitionBuilder definitionBuilder = new IndexDefinitionBuilder();
        definitionBuilder.setMap("from doc in docs.CommitInfos select new { doc.Revision }");
        store.getDatabaseCommands().putIndex(indexName, definitionBuilder, true);

        // wait for index
        session.query(CommitInfo.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000)).toList();

        QCommitInfo x = QCommitInfo.commitInfo;
        IRavenQueryable<CommitInfo> results = session.query(CommitInfo.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).where(x.revision.eq(1));
        //There is one CommitInfo with Revision == 1
        assertEquals(1, results.toList().size());

        results = session.query(CommitInfo.class).where(x.revision.eq(0));
        //There is not CommitInfo with Revision = 0 so hopefully we do not get any result
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.lt(1));
        //There are 0 CommitInfos which has Revision <1
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.lt(2));
        //There is one CommitInfo with Revision < 2
        assertEquals(1, results.toList().size());
        //Revision of resulted CommitInfo has to be 1
        CommitInfo cinfo = results.toList().get(0);
        assertEquals(1, cinfo.getRevision());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.loe(2));
        //There are 2 CommitInfos which has Revision <=2
        assertEquals(2, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(7));
        //There are 0 CommitInfos which has Revision >7
        assertEquals(0, results.toList().size());


        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(6));
        //There are 1 CommitInfos which has Revision >6
        assertEquals(1, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(6));
        //There are 2 CommitInfos which has Revision >=6
        assertEquals(2, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(6).and(x.revision.lt(6)));
        //There are 0 CommitInfos which has Revision >6 && <6
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(6).and(x.revision.loe(6)));
        //There are 1 CommitInfos which has Revision >=6 && <=6
        assertEquals(1, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(6).and(x.revision.lt(6)));
        //There are 0 CommitInfos which has Revision >=6 && <6
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(6).and(x.revision.loe(6)));
        //There are 0 CommitInfos which has Revision >6 && <=6
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(7).and(x.revision.loe(1)));
        //There are 0 CommitInfos which has Revision >=7  && <= 1
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(7).and(x.revision.lt(1)));
        //There are 0 CommitInfos which has Revision >7  && < 1
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(7).or(x.revision.lt(1)));
        //There are 0 CommitInfos which has Revision >7  || < 1
        assertEquals(0, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(7).or(x.revision.lt(1)));
        //There are 1 CommitInfos which has Revision >=7  || < 1
        assertEquals(1, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.gt(7).or(x.revision.loe(1)));
        //There are 1 CommitInfos which has Revision >7  || <= 1
        assertEquals(1, results.toList().size());

        results = session.query(CommitInfo.class, indexName)
            .where(x.revision.goe(7).or(x.revision.loe(1)));
        //There are 2 CommitInfos which has Revision >=7  || <= 1
        assertEquals(2, results.toList().size());

      }
    }
  }


  private final static String REPO = "/svn/repo/";

  private void addData(IDocumentSession session) {

    CommitInfo commit1 = new CommitInfo();
    commit1.setAuthor("kenny");
    commit1.setPathInRepo("/src/test/");
    commit1.setRepository(REPO);
    commit1.setRevision(1);
    commit1.setDate(new Date());
    commit1.setCommitMessage("First commit");
    session.store(commit1);

    CommitInfo commit2 = new CommitInfo();
    commit2.setAuthor("kenny");
    commit2.setPathInRepo("/src/test/FirstTest/");
    commit2.setRepository(REPO);
    commit2.setRevision(2);
    commit2.setDate(new Date());
    commit2.setCommitMessage("Second commit");
    session.store(commit2);

    CommitInfo commit3 = new CommitInfo();
    commit3.setAuthor("kenny");
    commit3.setPathInRepo("/src/test/FirstTest/test.txt");
    commit3.setRepository(REPO);
    commit3.setRevision(3);
    commit3.setDate(new Date());
    commit3.setCommitMessage("Third commit");
    session.store(commit3);

    CommitInfo commit4 = new CommitInfo();
    commit4.setAuthor("john");
    commit4.setPathInRepo("/src/test/SecondTest/");
    commit4.setRepository(REPO);
    commit4.setRevision(4);
    commit4.setDate(new Date());
    commit4.setCommitMessage("Fourth commit");
    session.store(commit4);

    CommitInfo commit5 = new CommitInfo();
    commit5.setAuthor("john");
    commit5.setPathInRepo("/src/");
    commit5.setRepository(REPO);
    commit5.setRevision(5);
    commit5.setDate(new Date());
    commit5.setCommitMessage("Fifth commit");
    session.store(commit5);

    CommitInfo commit6 = new CommitInfo();
    commit6.setAuthor("john");
    commit6.setPathInRepo("/src/test/SecondTest/test.txt");
    commit6.setRepository(REPO);
    commit6.setRevision(6);
    commit6.setDate(new Date());
    commit6.setCommitMessage("Sixth commit");
    session.store(commit6);

    CommitInfo commit7 = new CommitInfo();
    commit7.setAuthor("kenny");
    commit7.setPathInRepo("/src/test/SecondTest/test1.txt");
    commit7.setRepository(REPO);
    commit7.setRevision(7);
    commit7.setDate(new Date());
    commit7.setCommitMessage("Seventh commit");
    session.store(commit7);

    session.saveChanges();
  }
}
