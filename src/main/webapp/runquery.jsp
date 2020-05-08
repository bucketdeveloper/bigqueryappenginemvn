<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>
<div class="container">
    <h2>
        Query StackOverflow II
    </h2>

    <form method="POST" action="/query">

        <div>
            <label for="title">Query String</label>
            <input type="text" name="queryText" id="queryText" size="40" class="form-control" />
        </div>

        <div>
            <label for="results">Results</label>

           <c:forEach var="row" items="${requestScope.results}">
            <ul>
                <li><font color="#00CC00"><c:out value="${row.url}"/>, <c:out value="${row.viewCount}"/> </font></li>
            </ul>
           </c:forEach>
        </div>

        <button type="submit">Run Query</button>
    </form>
</div>