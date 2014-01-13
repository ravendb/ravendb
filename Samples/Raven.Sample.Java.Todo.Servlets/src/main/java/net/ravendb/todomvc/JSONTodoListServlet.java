package net.ravendb.todomvc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.ravendb.abstractions.commands.DeleteCommandData;
import net.ravendb.abstractions.commands.ICommandData;
import net.ravendb.abstractions.commands.PatchCommandData;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentSession;
import net.ravendb.client.linq.IRavenQueryable;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 *
 * @author Tyczo, AIS.PL
 *
 */
public class JSONTodoListServlet extends HttpServlet {

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException,
        IOException {
        ServletContext context = request.getSession().getServletContext();
        IDocumentStore store = (IDocumentStore) context.getAttribute(ContextManager.DOCUMENT_STORE);

        String[] ids = request.getParameterValues("id");

        if (ids != null) {
            try (DocumentSession session = (DocumentSession) store.openSession()) {

                List<ICommandData> commands = new ArrayList<>();

                for (String id : ids) {
                    DeleteCommandData patchCommand = new DeleteCommandData();

                    patchCommand.setKey(store.getConventions().defaultFindFullDocumentKeyFromNonStringIdentifier(
                        Integer.valueOf(id), Todo.class, false));

                    commands.add(patchCommand);
                }

                session.getDatabaseCommands().batch(commands);
                session.saveChanges();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        ServletContext context = request.getSession().getServletContext();
        IDocumentStore store = (IDocumentStore) context.getAttribute(ContextManager.DOCUMENT_STORE);

        String searchText = request.getParameter("search");

        try (IDocumentSession session = store.openSession()) {
            QTodo t = QTodo.todo;

            IRavenQueryable<Todo> query = session.query(Todo.class, TodoByTitleIndex.class)
                .orderBy(t.creationDate.asc())
                .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow());

            if (StringUtils.isNotBlank(searchText)) {
                query = query.where(t.title.eq(searchText));
            }

            List<Todo> todosList = query.toList();

            response.getWriter().write(RavenJArray.fromObject(todosList).toString());
            response.getWriter().close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
        IOException {
        ServletContext context = request.getSession().getServletContext();
        IDocumentStore store = (IDocumentStore) context.getAttribute(ContextManager.DOCUMENT_STORE);

        try (IDocumentSession session = store.openSession()) {
            Todo todo = new Todo(request.getParameter("title"));
            session.store(todo);
            session.saveChanges();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext context = request.getSession().getServletContext();
        IDocumentStore store = (IDocumentStore) context.getAttribute(ContextManager.DOCUMENT_STORE);

        String[] ids = request.getParameterValues("id");

        if (ids != null) {

            try (DocumentSession session = (DocumentSession) store.openSession()) {
                List<ICommandData> commands = new ArrayList<>();
                List<PatchRequest> patchRequests = new ArrayList<>();

                if (StringUtils.isNotBlank(request.getParameter("title"))) {
                    patchRequests.add(new PatchRequest(PatchCommandType.SET, "Title", new RavenJValue(request
                        .getParameter("title"))));
                }
                if (StringUtils.isNotBlank(request.getParameter("completed"))) {
                    patchRequests.add(new PatchRequest(PatchCommandType.SET, "Completed", new RavenJValue(Boolean
                        .valueOf(request.getParameter("completed")))));
                }

                for (String id : ids) {
                    PatchCommandData patchCommand = new PatchCommandData();

                    patchCommand.setKey(store.getConventions().defaultFindFullDocumentKeyFromNonStringIdentifier(
                        Integer.valueOf(id), Todo.class, false));
                    patchCommand.setPatches(patchRequests.toArray(new PatchRequest[patchRequests.size()]));

                    commands.add(patchCommand);
                }
                session.getDatabaseCommands().batch(commands);
                session.saveChanges();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
