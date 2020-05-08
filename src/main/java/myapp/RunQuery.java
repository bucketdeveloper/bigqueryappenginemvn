package myapp;

import com.google.cloud.bigquery.*;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.UUID;

@SuppressWarnings("serial")
@WebServlet(name = "query", value="/query")
public class RunQuery extends HttpServlet {

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        PrintWriter out = resp.getWriter();
        String queryText = req.getParameter("queryText");

//        out.println("Querying StackOverflow entries for text : " + queryText);
        System.out.println("Querying StackOverflow entries for text : " + queryText);

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT "
                                + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                                + "view_count "
                                + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                                + "WHERE tags like '%"+queryText+"%' "
                                + "ORDER BY favorite_count DESC LIMIT 10")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

// Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

// Wait for the query to complete.
        try {
            queryJob = queryJob.waitFor();
            // Get the results.
            TableResult result = queryJob.getQueryResults();
            ArrayList<BQResultRow> results = new ArrayList<>(0);
            for (FieldValueList row : result.iterateAll()) {
                String url = row.get("url").getStringValue();
                String viewCount = row.get("view_count").getStringValue();
                BQResultRow bqrow = new BQResultRow(url,viewCount);
                results.add(bqrow);
                System.out.printf("url: %s views: %s", url, viewCount);
            }
            req.setAttribute("results",results);
            req.getRequestDispatcher("/runquery.jsp?queryText="+ queryText +"").forward(req, resp);
        } catch (InterruptedException ie) {
            System.err.println(ie);
            System.exit(1);
        }

// Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            if (queryJob.getStatus().getError() != null)
                throw new RuntimeException(queryJob.getStatus().getError().toString());

    }


}