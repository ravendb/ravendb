package net.ravendb.todomvc;

import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentStore;

/**
 * @author Tyczo, AIS.PL
 *
 */
public class ContextManager implements ServletContextListener {

    public static final String DOCUMENT_STORE = "documentStore";

    private static Logger log = Logger.getLogger(ContextManager.class.getName());

    private static final String RAVENDB_PARAMETER_NAME = "ravenDB.url";

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        ServletContext ctx = event.getServletContext();
        IDocumentStore store = ((IDocumentStore) ctx.getAttribute(DOCUMENT_STORE));
        if (store != null) {
            ctx.removeAttribute(DOCUMENT_STORE);
            try {
                store.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        ServletContext ctx = event.getServletContext();

        String ravenDbUrl = System.getProperty(RAVENDB_PARAMETER_NAME);
        if (ravenDbUrl == null) {
            ravenDbUrl = ctx.getInitParameter(RAVENDB_PARAMETER_NAME);
        }
        if (ravenDbUrl == null) {
            throw new RuntimeException(RAVENDB_PARAMETER_NAME + " parameter is not defined");
        }

        IDocumentStore store = new DocumentStore(ravenDbUrl, "crud");
        store.initialize();
        store.executeIndex(new TodoByTitleIndex());

        ctx.setAttribute(DOCUMENT_STORE, store);

    }

}
