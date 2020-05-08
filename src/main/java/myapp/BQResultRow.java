package myapp;

public class BQResultRow {

    String url = "";
    String viewCount = "";

    public BQResultRow(String url, String viewCount) {
        this.url = url;
        this.viewCount = viewCount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getViewCount() {
        return viewCount;
    }

    public void setViewCount(String viewCount) {
        this.viewCount = viewCount;
    }
}
