package net.ravendb.todomvc;

import java.util.Date;

import com.mysema.query.annotations.QueryEntity;

/**
 * @author Tyczo, AIS.PL
 *
 */
@QueryEntity
public class Todo {

    private Boolean completed;
    private Date creationDate;
    private Integer id;
    private String title;

    /**
     * Constructs new instance
     *
     */
    public Todo() {
        super();
    }

    /**
     * Constructs new instance
     *
     * @param title the title
     */
    public Todo(final String title) {
        super();
        this.title = title;
        this.creationDate = new Date(System.currentTimeMillis());
    }

    /**
     * @return the completed
     */
    public Boolean getCompleted() {
        return completed;
    }

    /**
     * @return the creationDate
     */
    public Date getCreationDate() {
        return creationDate;
    }

    /**
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param completed the completed to set
     */
    public void setCompleted(Boolean completed) {
        this.completed = completed;
    }

    /**
     * @param creationDate the creationDate to set
     */
    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    /**
     * @param id the id to set
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "Todo [title=" + title + ", completed =" + completed + ", creationDate=" + creationDate + ", id=" + id
            + "]";
    }

}
