package myapp;

import com.google.appengine.repackaged.org.codehaus.jackson.map.ObjectMapper;
import com.google.cloud.bigquery.*;
import myapp.pojo.QueryOutput;
import myapp.pojo.QueryRow;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@SuppressWarnings("serial")
@WebServlet(name = "query", value="/query")
public class RunQuery extends HttpServlet {

    @Override
    /**
     * The Post is called by the runquery.jsp form
     */
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        PrintWriter out = resp.getWriter();
        String queryText = req.getParameter("queryText");

//        out.println("Querying StackOverflow entries for text : " + queryText);
        System.out.println("Querying StackOverflow entries for text ::: " + queryText);

        Job queryJob = getQueryJob(queryText);

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

    @Override
    /**
     * doGet is called by the tasks
     */
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        PrintWriter out = resp.getWriter();
        String queryText = req.getParameter("queryText");

//        out.println("Querying StackOverflow entries for text : " + queryText);
        System.out.println("Querying StackOverflow entries for text :: " + queryText);

        Job queryJob = getQueryJob(queryText);

        String taskName = req.getHeader("X-AppEngine-TaskName");
        if (taskName == null) {
            taskName = "Async Query From Browser";
        }
        System.out.println("Query task name: "+taskName);

// Wait for the query to complete.
        QueryOutput output = new QueryOutput();
        try {
            queryJob = queryJob.waitFor();
            // Get the results.
            TableResult result = queryJob.getQueryResults();

            output.setQuerytext(queryText);
            List<QueryRow> outputRows = new ArrayList<QueryRow>();
            for (FieldValueList row : result.iterateAll()) {
                String url = row.get("url").getStringValue();
                String viewCount = row.get("view_count").getStringValue();
                QueryRow rowOut = new QueryRow();
                rowOut.setUrl(url);
                rowOut.setViewCount(viewCount);
                outputRows.add(rowOut);
            }
            output.setRows(outputRows);

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


        // let's insert the result from the job
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream resultsText = new ByteArrayOutputStream();
        mapper.writeValue(resultsText, output);
        //String queryJobTitle, String queryTime, String queryResult
        String pattern = "E, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        String date = simpleDateFormat.format(new Date());
        queryJob = getInsertJob(taskName,date,resultsText.toString());

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            if (queryJob.getStatus().getError() != null)
                throw new RuntimeException(queryJob.getStatus().getError().toString());
        resp.getWriter().write("Job Submitted for task: "+taskName+"!");
        resp.getWriter().flush();
        resp.getWriter().close();

    }

    private Job getQueryJob(String queryText) {
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
        return bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    }

    private Job getInsertJob(String queryJobTitle, String queryTime, String queryResult) {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "INSERT INTO khtestsecondproject.xpodemo.bq_results VALUES('"
                                + queryJobTitle + "','"
                                + queryTime + "','"
                                + queryResult
                                + "')")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

// Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        return bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    }

}