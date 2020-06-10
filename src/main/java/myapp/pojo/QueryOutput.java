package myapp.pojo;

import java.util.List;

public class QueryOutput {

    private String querytext;
    private List<QueryRow> rows;

    public String getQuerytext() {
        return querytext;
    }

    public void setQuerytext(String querytext) {
        this.querytext = querytext;
    }

    public List<QueryRow> getRows() {
        return rows;
    }

    public void setRows(List<QueryRow> rows) {
        this.rows = rows;
    }
}
